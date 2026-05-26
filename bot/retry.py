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
