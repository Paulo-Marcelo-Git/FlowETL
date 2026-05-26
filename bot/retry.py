"""
Fila de retry automático para arquivos que falharam no ETL.
Classifica erros em 'infra' ou 'dado', persiste estado em tb_retry_queue
e reprocessa automaticamente com backoff exponencial.
"""

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
