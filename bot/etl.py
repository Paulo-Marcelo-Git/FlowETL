"""
ETL principal: lê arquivo .xlsx, valida, limpa e carrega no SQL Server.
Fluxo: Excel → staging → SP MERGE → produção → tb_log_etl
"""

import json
import os
import re
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from bot.alertas import alerta_falha_telegram, alerta_sucesso_telegram
from bot.database import carregar_staging, executar_merge, obter_db_config, obter_engine, sincronizar_colunas
from bot.logger import configurar_logger, registrar_log_banco

logger = configurar_logger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / 'config' / 'tabelas.json'
PROCESSADOS_DIR = BASE_DIR / 'processados'
ERROS_DIR = BASE_DIR / 'erros'
_IDENT_VALIDO = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


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


def _sanitizar_nome_coluna(nome: str) -> str:
    """Converte nome de coluna para snake_case seguro para SQL."""
    nome = nome.lower().strip()
    nome = re.sub(r'[^a-z0-9]', '_', nome)
    nome = re.sub(r'_+', '_', nome).strip('_')
    return nome


def _limpar_dataframe(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Aplica as regras de limpeza definidas no config da tabela."""
    # Dropar colunas lixo
    colunas_ignorar = cfg.get('colunas_ignorar', [])
    df = df.drop(columns=[c for c in colunas_ignorar if c in df.columns], errors='ignore')

    # Renomear colunas mapeadas no config
    colunas_renomear = cfg.get('colunas_renomear', {})
    df = df.rename(columns=colunas_renomear)

    # Sanitizar colunas desconhecidas (não estavam no mapeamento)
    colunas_mapeadas = set(colunas_renomear.values())
    novas = {col: _sanitizar_nome_coluna(col) for col in df.columns if col not in colunas_mapeadas}
    if novas:
        df = df.rename(columns=novas)
        logger.info(f'Colunas novas detectadas e sanitizadas: {novas}')

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


def processar_arquivo(caminho_arquivo: str, skip_retry: bool = False) -> bool:
    """
    Processa um único arquivo .xlsx.
    Retorna True em caso de sucesso, False em caso de falha.
    skip_retry=True evita enfileirar na fila de retry (usar em reprocessamento manual).
    """
    inicio = time.time()
    caminho = Path(caminho_arquivo)
    nm_arquivo = caminho.name

    logger.info(f'Iniciando processamento: {nm_arquivo}')

    config = _carregar_config()
    prefixo = _identificar_prefixo(nm_arquivo, config)

    if prefixo is not None:
        # Caminho 1: tabelas.json (comportamento original preservado)
        cfg_tabela = config[prefixo]
        nm_tabela  = cfg_tabela['tabela']
        aba_excel  = cfg_tabela.get('aba_excel', 0)
        chave      = cfg_tabela['chave']
        cfg_limpeza = cfg_tabela
        modo_discovery = False
    else:
        # Caminho 2: auto-discovery
        logger.info(f'Prefixo não encontrado em tabelas.json — iniciando auto-discovery para {nm_arquivo}')
        aba_excel   = 0
        cfg_limpeza = {}
        chave       = None
        nm_tabela   = nm_arquivo
        modo_discovery = True

    qt_recebidas = qt_inseridas = qt_rejeitadas = 0
    ds_erro = None

    try:
        logger.info(f'Lendo aba "{aba_excel}" de {nm_arquivo}')
        df = pd.read_excel(caminho, sheet_name=aba_excel, dtype=str)

        qt_recebidas = len(df)
        logger.info(f'{qt_recebidas} linhas brutas lidas.')

        df = _limpar_dataframe(df, cfg_limpeza)
        df['nm_arquivo_origem'] = nm_arquivo

        qt_rejeitadas = qt_recebidas - len(df)
        logger.info(f'Após limpeza: {len(df)} linhas válidas, {qt_rejeitadas} rejeitadas.')

        if modo_discovery:
            # Filtrar colunas com nomes inválidos para SQL (vazias, só números, etc.)
            colunas_validas = [c for c in df.columns if _IDENT_VALIDO.match(str(c))]
            colunas_removidas = [c for c in df.columns if c not in colunas_validas]
            if colunas_removidas:
                logger.info(f'Auto-discovery: removendo colunas com nomes inválidos: {colunas_removidas}')
                df = df[colunas_validas]

            from bot.schema_registry import buscar_match, criar_pipeline_novo
            match = buscar_match(list(df.columns))
            if match:
                nm_tabela = match['nm_tabela']
                chave     = match['nm_chave']
                db_config = {'staging': match['nm_staging'], 'sp_merge': match['nm_sp_merge']}
                logger.info(f'Auto-discovery: match com {nm_tabela}')
            else:
                logger.info(f'Auto-discovery: schema novo — criando pipeline para {nm_arquivo}')
                pipeline  = criar_pipeline_novo(nm_arquivo, list(df.columns))
                nm_tabela = pipeline['nm_tabela']
                chave     = pipeline['nm_chave']
                db_config = {'staging': pipeline['nm_staging'], 'sp_merge': pipeline['nm_sp_merge']}
                try:
                    from bot.dashboard_builder import criar_dashboard_automatico
                    from bot.schema_registry import atualizar_dashboard
                    dash_id, nm_dash = criar_dashboard_automatico(nm_tabela, list(df.columns))
                    if dash_id > 0:
                        atualizar_dashboard(nm_tabela, dash_id, nm_dash)
                except Exception as dash_exc:
                    logger.warning(f'Dashboard auto-criação falhou (não crítico): {dash_exc}')
        else:
            db_config = obter_db_config(nm_tabela)
            if db_config is None:
                msg = f'Tabela "{nm_tabela}" não encontrada em TABELA_CONFIG nem no schema_registry.'
                logger.error(msg)
                alerta_falha_telegram(nm_arquivo, msg)
                return False

        sincronizar_colunas(
            df,
            nm_staging=db_config['staging'],
            nm_producao=nm_tabela,
            nm_sp=db_config['sp_merge'],
            chave=chave,
        )

        carregar_staging(df, db_config['staging'])

        inseridas, atualizadas = executar_merge(db_config['sp_merge'])
        qt_inseridas = inseridas + atualizadas

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

        subpasta = datetime.now().strftime('%Y-%m')
        destino  = _mover_arquivo(caminho, PROCESSADOS_DIR, subpasta)
        logger.info(f'Arquivo movido para {destino}')

        alerta_sucesso_telegram(nm_arquivo, qt_inseridas)
        logger.info(f'Processamento concluído com sucesso: {nm_arquivo}')
        return True

    except Exception as exc:
        ds_erro = str(exc)
        logger.error(f'Erro ao processar {nm_arquivo}: {ds_erro}', exc_info=True)

        # 12. Mover para /erros/ e disparar alertas
        caminho_em_erros = ERROS_DIR / nm_arquivo  # fallback se o move falhar
        try:
            caminho_em_erros = _mover_arquivo(caminho, ERROS_DIR)
        except Exception as move_exc:
            logger.error(f'Falha ao mover arquivo para erros/: {move_exc}')

        try:
            engine = obter_engine()
            registrar_log_banco(
                engine=engine,
                nm_arquivo=nm_arquivo,
                nm_tabela_destino=nm_tabela,
                qt_linhas_recebidas=qt_recebidas,
                qt_linhas_inseridas=0,
                qt_linhas_rejeitadas=qt_recebidas,
                ds_status='falha',
                ds_erro=ds_erro,
                tm_duracao_seg=time.time() - inicio,
            )
        except Exception as log_exc:
            logger.error(f'Falha ao registrar log de erro no banco: {log_exc}')

        if not skip_retry:
            from bot.retry import enfileirar
            enfileirar(nm_arquivo, str(caminho_em_erros), exc)
        alerta_falha_telegram(nm_arquivo, ds_erro)
        return False
