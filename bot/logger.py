"""
Logging estruturado: arquivo rotativo + inserção na tb_log_etl.
"""

import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine


LOG_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')
LOG_FILE = os.path.join(LOG_DIR, 'flowetl.log')


def configurar_logger(nome: str = 'flowetl') -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger(nome)
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        '[%(asctime)s] %(levelname)-8s %(name)s — %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Handler arquivo rotativo (10 MB, 5 backups)
    fh = RotatingFileHandler(LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    # Handler console
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


def registrar_log_banco(
    engine: Engine,
    nm_arquivo: str,
    nm_tabela_destino: str,
    qt_linhas_recebidas: Optional[int],
    qt_linhas_inseridas: Optional[int],
    qt_linhas_rejeitadas: Optional[int],
    ds_status: str,
    ds_erro: Optional[str],
    tm_duracao_seg: Optional[float],
) -> None:
    """Insere uma linha de auditoria na tb_log_etl."""
    sql = text("""
        INSERT INTO dbo.tb_log_etl (
            nm_arquivo, nm_tabela_destino, dt_processamento,
            qt_linhas_recebidas, qt_linhas_inseridas, qt_linhas_rejeitadas,
            ds_status, ds_erro, tm_duracao_seg
        ) VALUES (
            :nm_arquivo, :nm_tabela_destino, :dt_processamento,
            :qt_linhas_recebidas, :qt_linhas_inseridas, :qt_linhas_rejeitadas,
            :ds_status, :ds_erro, :tm_duracao_seg
        )
    """)

    try:
        with engine.begin() as conn:
            conn.execute(sql, {
                'nm_arquivo': nm_arquivo,
                'nm_tabela_destino': nm_tabela_destino,
                'dt_processamento': datetime.now(),
                'qt_linhas_recebidas': qt_linhas_recebidas,
                'qt_linhas_inseridas': qt_linhas_inseridas,
                'qt_linhas_rejeitadas': qt_linhas_rejeitadas,
                'ds_status': ds_status,
                'ds_erro': ds_erro[:4000] if ds_erro else None,
                'tm_duracao_seg': round(tm_duracao_seg, 2) if tm_duracao_seg is not None else None,
            })
    except Exception as exc:
        logger = configurar_logger()
        logger.error(f'Falha ao gravar tb_log_etl: {exc}')
