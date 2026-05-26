"""
Registro de schemas para auto-descoberta de pipeline ETL.
Gerencia fingerprints de colunas, match de schemas conhecidos e
configuração de tabelas criadas dinamicamente.
"""

import json
import re
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import text

from bot.database import obter_engine
from bot.logger import configurar_logger

load_dotenv(override=True)
logger = configurar_logger(__name__)

_IDENT_RE = re.compile(r'^[A-Za-z0-9_]+$')
_MATCH_THRESHOLD = 0.8
_IGNORAR_COLUNAS = {'nm_arquivo_origem', 'dt_insert', 'dt_atualizacao'}


def _validar_identificador(valor: str, campo: str) -> None:
    if not _IDENT_RE.match(valor):
        raise ValueError(f"Identificador inválido para '{campo}': {valor!r}")


# ------------------------------------------------------------------ match

def buscar_match(colunas_df: list) -> Optional[dict]:
    """
    Busca schema em tb_schema_registry onde >= 80% das colunas base
    estão presentes em colunas_df. Retorna o dict do registro ou None.
    """
    colunas_set = set(colunas_df) - _IGNORAR_COLUNAS

    try:
        engine = obter_engine()
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT * FROM dbo.tb_schema_registry")
            ).mappings().all()
    except Exception as exc:
        logger.error(f'schema_registry.buscar_match: erro ao consultar banco: {exc}')
        return None

    melhor = None
    melhor_score = 0.0

    for row in rows:
        try:
            colunas_base = set(json.loads(row['colunas_base']))
        except Exception:
            continue
        if not colunas_base:
            continue
        score = len(colunas_set & colunas_base) / len(colunas_base)
        if score >= _MATCH_THRESHOLD and score > melhor_score:
            melhor_score = score
            melhor = dict(row)

    if melhor:
        logger.info(f'Schema match: {melhor["nm_tabela"]} (score={melhor_score:.0%})')
    return melhor


# ------------------------------------------------------------------ config lookup

def buscar_config_por_tabela(nm_tabela: str) -> Optional[dict]:
    """Retorna {'staging': ..., 'sp_merge': ...} para tabela no registry."""
    try:
        engine = obter_engine()
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT nm_staging, nm_sp_merge FROM dbo.tb_schema_registry WHERE nm_tabela = :t"),
                {'t': nm_tabela}
            ).mappings().fetchone()
        if row:
            return {'staging': row['nm_staging'], 'sp_merge': row['nm_sp_merge']}
    except Exception as exc:
        logger.error(f'schema_registry.buscar_config_por_tabela: {exc}')
    return None


# ------------------------------------------------------------------ registro

def registrar(
    nm_tabela: str,
    nm_staging: str,
    nm_sp_merge: str,
    nm_chave: str,
    colunas: list,
    id_dashboard_mb: Optional[int] = None,
    nm_dashboard_mb: Optional[str] = None,
) -> None:
    """Insere ou atualiza entrada no schema registry."""
    for valor, campo in [
        (nm_tabela,   'nm_tabela'),
        (nm_staging,  'nm_staging'),
        (nm_sp_merge, 'nm_sp_merge'),
        (nm_chave,    'nm_chave'),
    ]:
        _validar_identificador(valor, campo)

    colunas_base = json.dumps(sorted(c for c in colunas if c not in _IGNORAR_COLUNAS))
    try:
        engine = obter_engine()
        with engine.begin() as conn:
            existe = conn.execute(
                text("SELECT COUNT(*) FROM dbo.tb_schema_registry WHERE nm_tabela = :t"),
                {'t': nm_tabela}
            ).scalar()
            if existe:
                conn.execute(text("""
                    UPDATE dbo.tb_schema_registry
                    SET colunas_base    = :cols,
                        id_dashboard_mb = :id_dash,
                        nm_dashboard_mb = :nm_dash,
                        dt_atualizacao  = GETDATE()
                    WHERE nm_tabela = :t
                """), {'cols': colunas_base, 'id_dash': id_dashboard_mb,
                       'nm_dash': nm_dashboard_mb, 't': nm_tabela})
            else:
                conn.execute(text("""
                    INSERT INTO dbo.tb_schema_registry
                        (nm_tabela, nm_staging, nm_sp_merge, nm_chave,
                         colunas_base, id_dashboard_mb, nm_dashboard_mb)
                    VALUES (:t, :stg, :sp, :chave, :cols, :id_dash, :nm_dash)
                """), {
                    't': nm_tabela, 'stg': nm_staging, 'sp': nm_sp_merge,
                    'chave': nm_chave, 'cols': colunas_base,
                    'id_dash': id_dashboard_mb, 'nm_dash': nm_dashboard_mb,
                })
        logger.info(f'Schema registry: {nm_tabela} registrado/atualizado.')
    except Exception as exc:
        logger.error(f'schema_registry.registrar: {exc}')


def atualizar_dashboard(nm_tabela: str, id_dashboard_mb: int, nm_dashboard_mb: str) -> None:
    """Atualiza campos de dashboard após criação bem-sucedida."""
    try:
        engine = obter_engine()
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE dbo.tb_schema_registry
                SET id_dashboard_mb = :id_dash,
                    nm_dashboard_mb = :nm_dash,
                    dt_atualizacao  = GETDATE()
                WHERE nm_tabela = :t
            """), {'id_dash': id_dashboard_mb, 'nm_dash': nm_dashboard_mb, 't': nm_tabela})
    except Exception as exc:
        logger.error(f'schema_registry.atualizar_dashboard: {exc}')
