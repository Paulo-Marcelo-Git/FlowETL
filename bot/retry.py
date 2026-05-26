"""
Fila de retry automático para arquivos que falharam no ETL.
Classifica erros em 'infra' ou 'dado', persiste estado em tb_retry_queue
e reprocessa automaticamente com backoff exponencial.
"""

import os
from datetime import datetime, timedelta
from typing import Literal

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from bot.database import obter_engine
from bot.logger import configurar_logger

logger = configurar_logger(__name__)

# Intervalos de backoff em minutos, indexados pelo número da tentativa (0-based)
_BACKOFF: dict = {
    'infra': [1, 5, 30, 120, 480],
    'dado':  [5, 60],
}

_MAX_TENTATIVAS: dict = {
    'infra': 5,
    'dado':  2,
}

_ERROS_INFRA = (OperationalError, TimeoutError, ConnectionError, OSError)
_KEYWORDS_INFRA = ('timeout', 'connection', 'unavailable')


def classificar_erro(exc: Exception) -> Literal['infra', 'dado']:
    """Retorna 'infra' para erros transitórios de rede/banco, 'dado' para o resto."""
    if isinstance(exc, FileNotFoundError):
        # FileNotFoundError é subclasse de OSError — deve ser tratado como dado
        return 'dado'
    if isinstance(exc, _ERROS_INFRA):
        return 'infra'
    msg = str(exc).lower()
    if any(k in msg for k in _KEYWORDS_INFRA):
        return 'infra'
    return 'dado'


def _calcular_proxima_tentativa(tipo: str, tentativa: int) -> datetime:
    """
    Retorna o datetime da próxima tentativa.
    tentativa = índice 0-based: 0 = ainda não houve retry (calcula espera antes do primeiro).
    """
    intervalos = _BACKOFF.get(tipo, _BACKOFF['dado'])
    idx = min(tentativa, len(intervalos) - 1)
    return datetime.now() + timedelta(minutes=intervalos[idx])


def enfileirar(nm_arquivo: str, caminho: str, exc: Exception) -> None:
    """Insere arquivo na fila de retry. Idempotente: ignora se já está ativo na fila."""
    tipo = classificar_erro(exc)
    ds_erro = str(exc)[:4000]
    proxima = _calcular_proxima_tentativa(tipo, 0)
    max_tent = _MAX_TENTATIVAS[tipo]

    try:
        engine = obter_engine()
        with engine.begin() as conn:
            existe = conn.execute(
                text("""
                    SELECT COUNT(*) FROM dbo.tb_retry_queue
                    WHERE nm_arquivo = :nm
                      AND ds_status IN ('aguardando', 'processando')
                """),
                {'nm': nm_arquivo},
            ).scalar()
            if existe:
                logger.info(f'{nm_arquivo} já está na fila de retry — ignorado.')
                return
            conn.execute(
                text("""
                    INSERT INTO dbo.tb_retry_queue
                        (nm_arquivo, caminho_arquivo, tipo_erro, qt_tentativas,
                         qt_max_tentativas, dt_proxima_tentativa, ds_ultimo_erro, ds_status)
                    VALUES
                        (:nm, :caminho, :tipo, 0, :max_t, :proxima, :erro, 'aguardando')
                """),
                {
                    'nm': nm_arquivo, 'caminho': caminho, 'tipo': tipo,
                    'max_t': max_tent, 'proxima': proxima, 'erro': ds_erro,
                },
            )
        logger.info(f'{nm_arquivo} enfileirado para retry ({tipo}, próxima: {proxima}).')
    except Exception as db_exc:
        logger.error(f'Falha ao enfileirar {nm_arquivo}: {db_exc}')


def processar_fila() -> None:
    """Job APScheduler executado a cada 1 min.

    Fluxo por ciclo:
    1. Reseta entradas presas em 'processando' há mais de 30 min (recovery após crash).
    2. Busca entradas elegíveis (aguardando + dt_proxima_tentativa <= agora).
    3. Para cada entrada, chama _processar_entrada().
    """
    try:
        engine = obter_engine()
    except Exception as exc:
        logger.error(f'processar_fila: sem conexão ao banco: {exc}')
        return

    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE dbo.tb_retry_queue
                SET ds_status = 'aguardando'
                WHERE ds_status = 'processando'
                  AND dt_ultima_tentativa < DATEADD(MINUTE, -30, GETDATE())
            """))

        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id_retry, nm_arquivo, caminho_arquivo, tipo_erro,
                       qt_tentativas, qt_max_tentativas
                FROM dbo.tb_retry_queue
                WHERE ds_status = 'aguardando'
                  AND dt_proxima_tentativa <= GETDATE()
            """)).mappings().all()

        for row in rows:
            try:
                _processar_entrada(engine, dict(row))
            except Exception as entry_exc:
                logger.error(f'processar_fila: erro em {row["nm_arquivo"]}: {entry_exc}')

    except Exception as exc:
        logger.error(f'processar_fila: erro inesperado: {exc}')


def _processar_entrada(engine, row: dict) -> None:
    """Executa uma tentativa de retry para uma entrada da fila."""
    from bot.etl import processar_arquivo
    from bot.alertas import alerta_retry_sucesso, alerta_retry_desistiu

    id_retry = row['id_retry']
    nm_arquivo = row['nm_arquivo']
    caminho = row['caminho_arquivo']
    tipo = row['tipo_erro']
    qt = row['qt_tentativas']
    max_t = row['qt_max_tentativas']
    nova_tentativa = qt + 1

    # Arquivo sumiu de /erros/ — desiste imediatamente
    if not os.path.exists(caminho):
        msg = 'Arquivo não encontrado em /erros/ (removido ou reprocessado manualmente)'
        logger.warning(f'Retry: {nm_arquivo} — {msg}')
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE dbo.tb_retry_queue
                SET ds_status = 'desistiu', qt_tentativas = :qt,
                    ds_ultimo_erro = :msg, dt_ultima_tentativa = GETDATE()
                WHERE id_retry = :id
            """), {'qt': nova_tentativa, 'msg': msg, 'id': id_retry})
        alerta_retry_desistiu(nm_arquivo, tipo, nova_tentativa, max_t, msg)
        return

    # Marcar como processando para evitar reentrada no mesmo ciclo
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE dbo.tb_retry_queue
            SET ds_status = 'processando', dt_ultima_tentativa = GETDATE()
            WHERE id_retry = :id
        """), {'id': id_retry})

    try:
        sucesso = processar_arquivo(caminho)
        # Se processar_arquivo falhar internamente, seu próprio except chamará
        # enfileirar() — que é idempotente e ignorará pois a entrada está 'processando'.
        ds_erro = None if sucesso else 'processar_arquivo retornou False sem exceção'
    except Exception as exc:
        sucesso = False
        ds_erro = str(exc)[:4000]

    if sucesso:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM dbo.tb_retry_queue WHERE id_retry = :id"),
                {'id': id_retry},
            )
        alerta_retry_sucesso(nm_arquivo, nova_tentativa, max_t)
        logger.info(f'Retry sucesso: {nm_arquivo} (tentativa {nova_tentativa}/{max_t})')

    elif nova_tentativa >= max_t:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE dbo.tb_retry_queue
                SET ds_status = 'desistiu', qt_tentativas = :qt,
                    ds_ultimo_erro = :erro, dt_ultima_tentativa = GETDATE()
                WHERE id_retry = :id
            """), {'qt': nova_tentativa, 'erro': ds_erro, 'id': id_retry})
        alerta_retry_desistiu(nm_arquivo, tipo, nova_tentativa, max_t, ds_erro or '')
        logger.warning(f'Retry desistiu: {nm_arquivo} após {nova_tentativa} tentativas.')

    else:
        proxima = _calcular_proxima_tentativa(tipo, nova_tentativa)
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE dbo.tb_retry_queue
                SET ds_status = 'aguardando', qt_tentativas = :qt,
                    ds_ultimo_erro = :erro, dt_proxima_tentativa = :proxima,
                    dt_ultima_tentativa = GETDATE()
                WHERE id_retry = :id
            """), {'qt': nova_tentativa, 'erro': ds_erro, 'proxima': proxima, 'id': id_retry})
        logger.info(
            f'Retry falhou: {nm_arquivo} tentativa {nova_tentativa}/{max_t}, '
            f'próxima: {proxima}'
        )
