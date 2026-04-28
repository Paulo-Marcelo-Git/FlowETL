"""
CLI para reprocessamento manual de arquivos que falharam.

Uso:
    python scripts/reprocessar.py erros/gproblemas_abril_2024.xlsx
    python scripts/reprocessar.py --todos
"""

import argparse
import sys
from pathlib import Path

# Garante que o pacote bot seja encontrado ao executar da raiz do projeto
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

from bot.etl import processar_arquivo
from bot.logger import configurar_logger

load_dotenv()
logger = configurar_logger('reprocessar')

BASE_DIR = Path(__file__).resolve().parent.parent
ERROS_DIR = BASE_DIR / 'erros'


def reprocessar_arquivo(caminho: str) -> bool:
    path = Path(caminho)

    if not path.is_absolute():
        path = BASE_DIR / caminho

    if not path.exists():
        logger.error(f'Arquivo não encontrado: {path}')
        return False

    if not path.suffix.lower() in ('.xlsx', '.xls'):
        logger.error(f'Arquivo não é Excel: {path}')
        return False

    logger.info(f'Reprocessando: {path}')
    return processar_arquivo(str(path))


def reprocessar_todos() -> None:
    arquivos = list(ERROS_DIR.glob('*.xlsx')) + list(ERROS_DIR.glob('*.xls'))

    if not arquivos:
        logger.info('Nenhum arquivo encontrado em erros/.')
        return

    logger.info(f'{len(arquivos)} arquivo(s) encontrado(s) em erros/.')
    sucesso = 0
    falha = 0

    for arq in arquivos:
        ok = reprocessar_arquivo(str(arq))
        if ok:
            sucesso += 1
        else:
            falha += 1

    logger.info(f'Reprocessamento concluído: {sucesso} sucesso(s), {falha} falha(s).')


def main() -> None:
    parser = argparse.ArgumentParser(
        description='FlowETL — Reprocessamento manual de arquivos com falha'
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        'arquivo',
        nargs='?',
        help='Caminho do arquivo .xlsx a reprocessar (ex: erros/gproblemas_abril.xlsx)',
    )
    group.add_argument(
        '--todos',
        action='store_true',
        help='Reprocessar todos os arquivos presentes na pasta erros/',
    )
    args = parser.parse_args()

    if args.todos:
        reprocessar_todos()
    elif args.arquivo:
        ok = reprocessar_arquivo(args.arquivo)
        sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
