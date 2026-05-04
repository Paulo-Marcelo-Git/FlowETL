"""
Monitora erros do Metabase lendo os logs do container Docker.

Captura dois tipos de problema:
  - Erros de aplicação Metabase (validação de parâmetros, queries inválidas)
  - Exceções JDBC / SQL Server que o Metabase loga ao executar consultas

Fluxo:
  1. Lê as últimas N linhas do container 'metabase' via docker logs
  2. Filtra linhas ERROR / WARN relevantes + stack traces associados
  3. Persiste novidades em tb_log_metabase
  4. Alerta Telegram se houver erros críticos
"""

import re
import subprocess
from datetime import datetime
from typing import List, Dict, Optional

from sqlalchemy import text

from bot.database import obter_engine
from bot.alertas import enviar_telegram
from bot.logger import configurar_logger

logger = configurar_logger(__name__)

CONTAINER_METABASE = 'metabase'

# Formato de linha de log Metabase: "2026-05-04 12:01:46,116 ERROR modulo :: msg"
_RE_LINHA = re.compile(
    r'^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+'
    r'(?P<nivel>ERROR|WARN)\s+'
    r'(?P<modulo>\S+)\s+::\s+'
    r'(?P<msg>.+)$'
)

# Palavras-chave que tornam uma linha WARN relevante (ERRORs sempre são capturados)
_RELEVANTE = re.compile(
    r'sql|query|exception|jdbc|sqlserver|invalid|parameter|template.tag|'
    r'order by|should be a string|non-blank',
    re.IGNORECASE,
)

# Linhas de stack trace que enriquecem o contexto do erro anterior
_STACK_TRACE = re.compile(r'^\s*(at |Caused by:|com\.microsoft\.|java\.)')


def _ler_logs_docker(linhas: int = 2000) -> List[str]:
    """Retorna as últimas N linhas do container Metabase."""
    try:
        result = subprocess.run(
            ['docker', 'logs', CONTAINER_METABASE, '--tail', str(linhas)],
            capture_output=True, text=True, timeout=20,
        )
        return (result.stdout + result.stderr).splitlines()
    except FileNotFoundError:
        logger.warning('docker não encontrado no PATH — monitor Metabase desativado.')
        return []
    except subprocess.TimeoutExpired:
        logger.warning('Timeout ao ler logs do container Metabase.')
        return []
    except Exception as exc:
        logger.error(f'Erro ao ler logs Docker: {exc}')
        return []


def _ultima_captura() -> Optional[datetime]:
    """Retorna o dt_evento do último registro gravado em tb_log_metabase."""
    try:
        engine = obter_engine()
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT MAX(dt_evento) FROM dbo.tb_log_metabase WHERE fonte = 'docker'")
            ).fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def _parsear_eventos(linhas: List[str], desde: Optional[datetime]) -> List[Dict]:
    """
    Percorre as linhas e agrupa cada erro com até 5 linhas de stack trace seguintes.
    Filtra apenas eventos posteriores a `desde`.
    """
    eventos: List[Dict] = []
    i = 0
    while i < len(linhas):
        m = _RE_LINHA.match(linhas[i])
        if not m:
            i += 1
            continue

        nivel = m.group('nivel')
        msg   = m.group('msg')

        # WARNs só entram se forem relevantes para SQL/queries
        if nivel == 'WARN' and not _RELEVANTE.search(msg):
            i += 1
            continue

        ts_str = m.group('ts')
        try:
            dt_evento = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            dt_evento = None

        # Ignorar eventos já processados
        if desde and dt_evento and dt_evento <= desde:
            i += 1
            continue

        # Coletar linhas de stack trace logo abaixo
        stack: List[str] = []
        j = i + 1
        while j < len(linhas) and j < i + 6 and _STACK_TRACE.match(linhas[j]):
            stack.append(linhas[j].strip())
            j += 1

        detalhe = msg
        if stack:
            detalhe += '\n' + '\n'.join(stack)

        eventos.append({
            'fonte':      'docker',
            'nivel':      nivel,
            'modulo':     m.group('modulo'),
            'mensagem':   detalhe[:4000],
            'dt_evento':  dt_evento,
        })
        i = j

    return eventos


def _gravar_eventos(eventos: List[Dict]) -> int:
    """Insere eventos em tb_log_metabase. Retorna qtd gravada."""
    if not eventos:
        return 0

    engine = obter_engine()
    sql = text("""
        INSERT INTO dbo.tb_log_metabase (fonte, nivel, modulo, mensagem, dt_evento)
        VALUES (:fonte, :nivel, :modulo, :mensagem, :dt_evento)
    """)
    gravados = 0
    with engine.begin() as conn:
        for ev in eventos:
            try:
                conn.execute(sql, ev)
                gravados += 1
            except Exception as exc:
                logger.error(f'Falha ao gravar evento em tb_log_metabase: {exc}')
    return gravados


def verificar_e_alertar() -> None:
    """
    Ponto de entrada chamado pelo APScheduler a cada 5 minutos.
    Lê logs do Docker, extrai erros novos, persiste e alerta Telegram.
    """
    linhas    = _ler_logs_docker(linhas=2000)
    desde     = _ultima_captura()
    eventos   = _parsear_eventos(linhas, desde)

    gravados  = _gravar_eventos(eventos)

    erros_criticos = [e for e in eventos if e['nivel'] == 'ERROR']

    if gravados:
        logger.info(f'Monitor Metabase: {gravados} evento(s) registrado(s) '
                    f'({len(erros_criticos)} error(s)).')

    if erros_criticos:
        amostra = erros_criticos[0]['mensagem'][:400]
        mensagem = (
            '<b>⚠️ FlowETL — Erro no Metabase</b>\n'
            f'🔴 {len(erros_criticos)} erro(s) detectado(s)\n'
            f'📋 <code>{amostra}</code>\n'
            f'⏱️ {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
        )
        enviar_telegram(mensagem)
        logger.warning(
            f'Monitor Metabase: {len(erros_criticos)} erro(s) crítico(s) — '
            'alerta Telegram enviado.'
        )
