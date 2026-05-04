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


def _reconstruir_sp_merge(nm_staging: str, nm_producao: str, nm_sp: str, chave: str) -> None:
    """Reconstrói a SP de MERGE com base nas colunas atuais da tabela staging."""
    engine = obter_engine()
    ignorar = {chave, 'dt_insert', 'dt_atualizacao'}

    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(:t) ORDER BY column_id"),
            {'t': f'dbo.{nm_staging}'},
        )
        colunas = [row[0] for row in result if row[0] not in ignorar]

    cols_select = ',\n            '.join(f'[{c}]' for c in [chave] + colunas)
    cols_update = ',\n            '.join(f'destino.[{c}] = origem.[{c}]' for c in colunas)
    cols_insert = ', '.join(f'[{c}]' for c in [chave] + colunas + ['dt_insert'])
    vals_insert = ', '.join(f'origem.[{c}]' for c in [chave] + colunas) + ', GETDATE()'

    create_sql = f"""
CREATE PROCEDURE dbo.{nm_sp}
    @linhas_inseridas   INT OUTPUT,
    @linhas_atualizadas INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @acao TABLE (tipo_acao VARCHAR(10));

    MERGE dbo.{nm_producao} AS destino
    USING (
        SELECT
            {cols_select}
        FROM dbo.{nm_staging}
        WHERE [{chave}] IS NOT NULL
    ) AS origem
    ON destino.[{chave}] = origem.[{chave}]

    WHEN MATCHED THEN
        UPDATE SET
            {cols_update},
            destino.dt_atualizacao = GETDATE()

    WHEN NOT MATCHED BY TARGET THEN
        INSERT ({cols_insert})
        VALUES ({vals_insert})

    OUTPUT $action INTO @acao;

    SELECT @linhas_inseridas   = COUNT(*) FROM @acao WHERE tipo_acao = 'INSERT';
    SELECT @linhas_atualizadas = COUNT(*) FROM @acao WHERE tipo_acao = 'UPDATE';
END;
"""

    with engine.begin() as conn:
        conn.execute(text(f"IF OBJECT_ID('dbo.{nm_sp}', 'P') IS NOT NULL DROP PROCEDURE dbo.{nm_sp}"))
        conn.execute(text(create_sql))

    logger.info(f'SP {nm_sp} recriada com {len(colunas)} colunas.')


def sincronizar_colunas(
    df: pd.DataFrame,
    nm_staging: str,
    nm_producao: str,
    nm_sp: str,
    chave: str,
) -> None:
    """
    Compara colunas do DataFrame com a tabela staging.
    Adiciona colunas novas via ALTER TABLE e recria a SP de MERGE.
    """
    engine = obter_engine()
    controle = {'dt_insert', 'dt_atualizacao'}

    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT LOWER(name) FROM sys.columns WHERE object_id = OBJECT_ID(:t)"),
            {'t': f'dbo.{nm_staging}'},
        )
        existentes = {row[0] for row in result}

    novas = set(df.columns) - existentes - controle
    if not novas:
        return

    logger.info(f'Novas colunas detectadas: {sorted(novas)} — atualizando banco...')

    with engine.begin() as conn:
        for col in sorted(novas):
            conn.execute(text(f'ALTER TABLE dbo.{nm_staging} ADD [{col}] VARCHAR(MAX) NULL'))
            conn.execute(text(f'ALTER TABLE dbo.{nm_producao} ADD [{col}] VARCHAR(MAX) NULL'))
            logger.info(f'Coluna [{col}] adicionada a {nm_staging} e {nm_producao}.')

    _reconstruir_sp_merge(nm_staging, nm_producao, nm_sp, chave)


# Mapeamento: tabela de produção → staging e SP
TABELA_CONFIG = {
    'tb_problemas_gov_ti': {
        'staging': 'stg_problemas_gov_ti',
        'sp_merge': 'sp_merge_problemas_gov_ti',
    }
}
