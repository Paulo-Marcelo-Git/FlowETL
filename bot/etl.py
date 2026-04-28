"""
ETL principal: lê arquivo .xlsx, valida, limpa e carrega no SQL Server.
Fluxo: Excel → staging → SP MERGE → produção → tb_log_etl
"""

import json
import os
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from bot.alertas import alerta_falha_telegram, alerta_sucesso_telegram
from bot.database import TABELA_CONFIG, carregar_staging, executar_merge, obter_engine
from bot.logger import configurar_logger, registrar_log_banco

logger = configurar_logger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / 'config' / 'tabelas.json'
PROCESSADOS_DIR = BASE_DIR / 'processados'
ERROS_DIR = BASE_DIR / 'erros'


def _carregar_config() -> dict:
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def _identificar_prefixo(nm_arquivo: str, config: dict) -> Optional[str]:
    """Retorna o prefixo do config que corresponde ao nome do arquivo."""
    nome_lower = nm_arquivo.lower()
    for prefixo in config:
        if nome_lower.startswith(prefixo.lower()):
            return prefixo
    return None


def _mover_arquivo(origem: Path, destino_dir: Path, subpasta: Optional[str] = None) -> Path:
    """Move arquivo para destino_dir/subpasta/, criando pastas se necessário."""
    if subpasta:
        destino_dir = destino_dir / subpasta
    destino_dir.mkdir(parents=True, exist_ok=True)
    destino = destino_dir / origem.name
    shutil.move(str(origem), str(destino))
    return destino


def _limpar_dataframe(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Aplica as regras de limpeza definidas no config da tabela."""
    # Dropar colunas lixo
    colunas_ignorar = cfg.get('colunas_ignorar', [])
    df = df.drop(columns=[c for c in colunas_ignorar if c in df.columns], errors='ignore')

    # Renomear colunas
    colunas_renomear = cfg.get('colunas_renomear', {})
    df = df.rename(columns=colunas_renomear)

    # Dropar linhas completamente vazias
    df = df.dropna(how='all')

    # Converter dt_abertura para DATE (None se inválido)
    if 'dt_abertura' in df.columns:
        df['dt_abertura'] = pd.to_datetime(df['dt_abertura'], errors='coerce').dt.date

    # dt_conclusao permanece como VARCHAR — só converter para string
    if 'dt_conclusao' in df.columns:
        df['dt_conclusao'] = df['dt_conclusao'].astype(str).str.strip()
        df['dt_conclusao'] = df['dt_conclusao'].replace({'nan': None, 'NaT': None, '': None})

    # Garantir que numero seja inteiro
    if 'numero' in df.columns:
        df['numero'] = pd.to_numeric(df['numero'], errors='coerce')
        df = df.dropna(subset=['numero'])
        df['numero'] = df['numero'].astype(int)

    return df


def processar_arquivo(caminho_arquivo: str) -> bool:
    """
    Processa um único arquivo .xlsx.
    Retorna True em caso de sucesso, False em caso de falha.
    """
    inicio = time.time()
    caminho = Path(caminho_arquivo)
    nm_arquivo = caminho.name

    logger.info(f'Iniciando processamento: {nm_arquivo}')

    config = _carregar_config()
    prefixo = _identificar_prefixo(nm_arquivo, config)

    if prefixo is None:
        msg = f'Nenhum prefixo em tabelas.json corresponde ao arquivo "{nm_arquivo}". Ignorado.'
        logger.warning(msg)
        return False

    cfg_tabela = config[prefixo]
    nm_tabela = cfg_tabela['tabela']
    aba_excel = cfg_tabela.get('aba_excel', 0)

    db_config = TABELA_CONFIG.get(nm_tabela)
    if db_config is None:
        msg = f'Tabela "{nm_tabela}" não encontrada em database.TABELA_CONFIG.'
        logger.error(msg)
        alerta_falha_telegram(nm_arquivo, msg)
        return False

    qt_recebidas = qt_inseridas = qt_rejeitadas = 0
    ds_erro = None

    try:
        # 1-2. Ler xlsx na aba correta
        logger.info(f'Lendo aba "{aba_excel}" de {nm_arquivo}')
        df = pd.read_excel(caminho, sheet_name=aba_excel, dtype=str)

        qt_recebidas = len(df)
        logger.info(f'{qt_recebidas} linhas brutas lidas.')

        # 3-7. Limpar e transformar
        df = _limpar_dataframe(df, cfg_tabela)

        # Adicionar coluna de rastreabilidade
        df['nm_arquivo_origem'] = nm_arquivo

        qt_rejeitadas = qt_recebidas - len(df)
        logger.info(f'Após limpeza: {len(df)} linhas válidas, {qt_rejeitadas} rejeitadas.')

        # 8. Inserir na staging
        carregar_staging(df, db_config['staging'])

        # 9. Executar MERGE
        inseridas, atualizadas = executar_merge(db_config['sp_merge'])
        qt_inseridas = inseridas + atualizadas

        # 10. Registrar log no banco
        engine = obter_engine()
        registrar_log_banco(
            engine=engine,
            nm_arquivo=nm_arquivo,
            nm_tabela_destino=nm_tabela,
            qt_linhas_recebidas=qt_recebidas,
            qt_linhas_inseridas=qt_inseridas,
            qt_linhas_rejeitadas=qt_rejeitadas,
            ds_status='sucesso',
            ds_erro=None,
            tm_duracao_seg=time.time() - inicio,
        )

        # 11. Mover para /processados/YYYY-MM/
        subpasta = datetime.now().strftime('%Y-%m')
        destino = _mover_arquivo(caminho, PROCESSADOS_DIR, subpasta)
        logger.info(f'Arquivo movido para {destino}')

        alerta_sucesso_telegram(nm_arquivo, qt_inseridas)
        logger.info(f'Processamento concluído com sucesso: {nm_arquivo}')
        return True

    except Exception as exc:
        ds_erro = str(exc)
        logger.error(f'Erro ao processar {nm_arquivo}: {ds_erro}', exc_info=True)

        # 12. Mover para /erros/ e disparar alertas
        try:
            _mover_arquivo(caminho, ERROS_DIR)
        except Exception as move_exc:
            logger.error(f'Falha ao mover arquivo para erros/: {move_exc}')

        try:
            engine = obter_engine()
            registrar_log_banco(
                engine=engine,
                nm_arquivo=nm_arquivo,
                nm_tabela_destino=nm_tabela if 'nm_tabela' in dir() else 'desconhecida',
                qt_linhas_recebidas=qt_recebidas,
                qt_linhas_inseridas=0,
                qt_linhas_rejeitadas=qt_recebidas,
                ds_status='falha',
                ds_erro=ds_erro,
                tm_duracao_seg=time.time() - inicio,
            )
        except Exception as log_exc:
            logger.error(f'Falha ao registrar log de erro no banco: {log_exc}')

        alerta_falha_telegram(nm_arquivo, ds_erro)
        return False
