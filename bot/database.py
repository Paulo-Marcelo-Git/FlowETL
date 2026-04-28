"""
Conexão com SQL Server via SQLAlchemy e operações de banco de dados:
- truncate + bulk insert na staging
- execução da SP de MERGE
"""

import os
from typing import Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from bot.logger import configurar_logger

load_dotenv()

logger = configurar_logger(__name__)

_engine: Optional[Engine] = None


def obter_engine() -> Engine:
    """Retorna o engine singleton, criando na primeira chamada."""
    global _engine
    if _engine is None:
        conn_str = os.getenv('SQL_SERVER_CONN')
        if not conn_str:
            raise EnvironmentError('Variável SQL_SERVER_CONN não definida no .env')
        try:
            _engine = create_engine(conn_str, fast_executemany=True)
            logger.info('Engine SQL Server criado com sucesso.')
        except Exception as exc:
            logger.error(f'Erro ao criar engine SQL Server: {exc}')
            raise
    return _engine


def carregar_staging(df: pd.DataFrame, nm_tabela_staging: str) -> int:
    """
    Trunca a tabela staging e insere o DataFrame.
    Retorna o número de linhas inseridas.
    """
    engine = obter_engine()

    try:
        with engine.begin() as conn:
            conn.execute(text(f'TRUNCATE TABLE dbo.{nm_tabela_staging}'))
            logger.info(f'Tabela {nm_tabela_staging} truncada.')

        df.to_sql(
            name=nm_tabela_staging,
            con=engine,
            schema='dbo',
            if_exists='append',
            index=False,
            chunksize=500,
            method='multi',
        )
        logger.info(f'{len(df)} linhas inseridas em {nm_tabela_staging}.')
        return len(df)

    except Exception as exc:
        logger.error(f'Erro ao carregar staging {nm_tabela_staging}: {exc}')
        raise


def executar_merge(nm_sp: str) -> Tuple[int, int]:
    """
    Executa a stored procedure de MERGE.
    Retorna (linhas_inseridas, linhas_atualizadas).
    """
    engine = obter_engine()

    try:
        with engine.begin() as conn:
            result = conn.execute(
                text(f"""
                    DECLARE @ins INT, @upd INT;
                    EXEC dbo.{nm_sp} @linhas_inseridas = @ins OUTPUT, @linhas_atualizadas = @upd OUTPUT;
                    SELECT @ins AS inseridas, @upd AS atualizadas;
                """)
            )
            row = result.fetchone()
            inseridas = int(row.inseridas) if row and row.inseridas is not None else 0
            atualizadas = int(row.atualizadas) if row and row.atualizadas is not None else 0
            logger.info(f'MERGE {nm_sp}: {inseridas} inseridas, {atualizadas} atualizadas.')
            return inseridas, atualizadas

    except Exception as exc:
        logger.error(f'Erro ao executar SP {nm_sp}: {exc}')
        raise


# Mapeamento: tabela de produção → staging e SP
TABELA_CONFIG = {
    'tb_problemas_gov_ti': {
        'staging': 'stg_problemas_gov_ti',
        'sp_merge': 'sp_merge_problemas_gov_ti',
    }
}
