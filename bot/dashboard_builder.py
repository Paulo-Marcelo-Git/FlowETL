"""
Criação automática de dashboards no Metabase baseada nas colunas da tabela.
Detecta padrões de colunas e gera cards apropriados (bar, pie, scalar).
"""

import os
import re

import requests
import urllib3
from dotenv import load_dotenv

from bot.logger import configurar_logger

load_dotenv(override=True)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = configurar_logger(__name__)

_IDENT_RE = re.compile(r'^[A-Za-z0-9_]+$')

# Padrões: (lista de nomes candidatos, tipo display, título do card)
_PADROES = [
    (['status'],                                      'bar', 'Por Status'),
    (['prioridade'],                                  'pie', 'Por Prioridade'),
    (['gerente_responsavel', 'gerente', 'responsavel'], 'bar', 'Por Responsável'),
    (['departamento_relator', 'departamento'],         'bar', 'Por Departamento'),
    (['sistema'],                                     'bar', 'Por Sistema'),
    (['jornada_impactada', 'jornada'],                'bar', 'Por Jornada'),
    (['paliativo'],                                   'pie', 'Por Paliativo'),
    (['tipo'],                                        'pie', 'Por Tipo'),
    (['categoria'],                                   'bar', 'Por Categoria'),
    (['origem'],                                      'bar', 'Por Origem'),
]

_IGNORAR = {'nm_arquivo_origem', 'dt_insert', 'dt_atualizacao'}


def _autenticar() -> tuple:
    """Retorna (base_url, session_token). Tenta IP interno e depois MB_SITE_URL."""
    usuario = os.getenv('MB_ADMIN_USER')
    senha   = os.getenv('MB_ADMIN_PASS')
    urls = ['http://172.18.0.2:3000',
            os.getenv('MB_SITE_URL', 'https://localhost').rstrip('/')]
    for url in urls:
        try:
            r = requests.post(f'{url}/api/session',
                              json={'username': usuario, 'password': senha},
                              verify=False, timeout=10)
            if r.status_code == 200:
                return url, r.json()['id']
        except Exception:
            continue
    raise RuntimeError('Não foi possível autenticar no Metabase')


def _obter_db_id(base: str, headers: dict) -> int:
    """Retorna o id do banco SQL Server configurado no Metabase."""
    r = requests.get(f'{base}/api/database', headers=headers, verify=False, timeout=10)
    r.raise_for_status()
    display_name = os.getenv('MB_DB_DISPLAY_NAME', 'FlowETL - SQL Server')
    dbs = r.json().get('data', [])
    for db in dbs:
        if db['name'] == display_name:
            return db['id']
    for db in dbs:
        if db['engine'] == 'sqlserver':
            return db['id']
    raise RuntimeError(f'Banco SQL Server não encontrado no Metabase (procurado: "{display_name}")')


def _col_bate(col: str, padroes: list) -> bool:
    return any(col == p or col.startswith(p + '_') or col.endswith('_' + p)
               for p in padroes)


def _inferir_cards(colunas: list, nm_tabela: str, db_id: int) -> list:
    """Gera specs de cards com base nos padrões de colunas detectados."""
    uteis = [c for c in colunas if c not in _IGNORAR and not c.startswith('status_')]

    cards = [{
        'name': 'Total de Registros',
        'query': f'SELECT COUNT(*) AS total FROM dbo.{nm_tabela}',
        'display': 'scalar',
        'viz': {},
        'db_id': db_id,
    }]

    for padroes, display, titulo in _PADROES:
        col = next((c for c in uteis if _col_bate(c, padroes)), None)
        if not col:
            continue
        if not _IDENT_RE.match(col):
            logger.warning(f'_inferir_cards: coluna inválida ignorada: {col!r}')
            continue
        if display == 'bar':
            query = (f'SELECT [{col}], COUNT(*) AS qt FROM dbo.{nm_tabela} '
                     f'WHERE [{col}] IS NOT NULL GROUP BY [{col}] ORDER BY qt DESC')
            viz = {'graph.dimensions': [col], 'graph.metrics': ['qt']}
        else:
            query = (f'SELECT [{col}], COUNT(*) AS qt FROM dbo.{nm_tabela} '
                     f'WHERE [{col}] IS NOT NULL GROUP BY [{col}]')
            viz = {'pie.dimension': col, 'pie.metric': 'qt'}
        cards.append({'name': titulo, 'query': query, 'display': display,
                      'viz': viz, 'db_id': db_id})

    return cards


def _criar_card(base: str, headers: dict, spec: dict) -> int:
    """Cria um card nativo no Metabase. Retorna o ID criado."""
    payload = {
        'name': spec['name'],
        'display': spec['display'],
        'database_id': spec['db_id'],
        'dataset_query': {
            'type': 'native',
            'native': {'query': spec['query']},
            'database': spec['db_id'],
        },
        'visualization_settings': spec.get('viz', {}),
        'collection_id': None,
    }
    r = requests.post(f'{base}/api/card', headers=headers,
                      json=payload, verify=False, timeout=15)
    r.raise_for_status()
    return r.json()['id']


def criar_dashboard_automatico(nm_tabela: str, colunas: list) -> tuple:
    """
    Cria dashboard Metabase com cards gerados automaticamente.
    Retorna (dashboard_id, dashboard_name).
    Em caso de erro retorna (-1, nome_esperado) — falha não é crítica para o ETL.
    """
    nm_dash = f'FlowETL — {nm_tabela}'

    try:
        base, token = _autenticar()
        headers = {'X-Metabase-Session': token}
        db_id = _obter_db_id(base, headers)

        # Criar cards
        specs = _inferir_cards(colunas, nm_tabela, db_id)
        card_ids = []
        for spec in specs:
            try:
                cid = _criar_card(base, headers, spec)
                card_ids.append(cid)
                logger.info(f'Card criado: "{spec["name"]}" (id={cid})')
            except Exception as exc:
                logger.warning(f'Falha ao criar card "{spec["name"]}": {exc}')

        if not card_ids:
            raise RuntimeError('Nenhum card criado com sucesso — dashboard não será criado')

        # Criar dashboard
        r = requests.post(f'{base}/api/dashboard', headers=headers,
                          json={'name': nm_dash, 'collection_id': None},
                          verify=False, timeout=10)
        r.raise_for_status()
        dash_id = r.json()['id']

        # Montar layout: scalar (total) ocupa linha inteira, demais em 2 colunas
        dashcards = []
        for i, cid in enumerate(card_ids):
            if i == 0:
                row, col_pos, size_x, size_y = 0, 0, 24, 4
            else:
                j = i - 1
                row     = 4 + (j // 2) * 6
                col_pos = (j % 2) * 12
                size_x  = 12
                size_y  = 6
            dashcards.append({
                'id': -(i + 1),
                'card_id': cid,
                'row': row, 'col': col_pos,
                'size_x': size_x, 'size_y': size_y,
                'parameter_mappings': [],
                'visualization_settings': {},
                'series': [],
            })

        r = requests.put(f'{base}/api/dashboard/{dash_id}', headers=headers,
                         json={'dashcards': dashcards, 'parameters': []},
                         verify=False, timeout=15)
        r.raise_for_status()

        logger.info(f'Dashboard "{nm_dash}" criado (id={dash_id}, cards={len(card_ids)}).')
        return dash_id, nm_dash

    except Exception as exc:
        logger.error(f'dashboard_builder.criar_dashboard_automatico: {exc}')
        return -1, nm_dash
