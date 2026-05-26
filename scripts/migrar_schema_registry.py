"""
Seed inicial do tb_schema_registry a partir do estado atual do banco.
Executar uma única vez após deploy da Task 1-5.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv(override=True)

from bot.database import obter_engine
from bot.schema_registry import registrar
from bot.logger import configurar_logger

logger = configurar_logger('migrar_schema_registry')

PIPELINES_EXISTENTES = [
    {
        'nm_tabela':   'tb_problemas_gov_ti',
        'nm_staging':  'stg_problemas_gov_ti',
        'nm_sp_merge': 'sp_merge_problemas_gov_ti',
        'nm_chave':    'numero',
    },
]


def main():
    engine = obter_engine()
    for p in PIPELINES_EXISTENTES:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT LOWER(name) AS name
                FROM sys.columns
                WHERE object_id = OBJECT_ID(:t)
                  AND name NOT IN ('dt_insert', 'dt_atualizacao')
                ORDER BY column_id
            """), {'t': f'dbo.{p["nm_tabela"]}'}).fetchall()

        colunas = [r[0] for r in rows]
        if not colunas:
            logger.warning(f'Tabela {p["nm_tabela"]} não encontrada — ignorada.')
            continue

        registrar(
            nm_tabela=p['nm_tabela'],
            nm_staging=p['nm_staging'],
            nm_sp_merge=p['nm_sp_merge'],
            nm_chave=p['nm_chave'],
            colunas=colunas,
        )
        logger.info(f'{p["nm_tabela"]}: {len(colunas)} colunas registradas.')

    print('Migração concluída.')


if __name__ == '__main__':
    main()
