# Auto-Descoberta de Schema e Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Substituir o lookup por prefixo em `tabelas.json` por descoberta automática de schema via fingerprint de colunas, criando tabelas SQL e dashboards Metabase automaticamente quando um novo tipo de planilha for detectado.

**Architecture:** `tabelas.json` continua como override explícito (prioridade 1). Um novo registro `tb_schema_registry` armazena fingerprints de schemas conhecidos (prioridade 2). Quando nenhum match é encontrado (threshold ≥ 80% das colunas base), o pipeline completo é criado automaticamente: tabelas SQL + SP MERGE + dashboard Metabase. O ETL existente é preservado integralmente para arquivos com prefixo em `tabelas.json`.

**Tech Stack:** Python (SQLAlchemy, pandas, requests), SQL Server DDL dinâmico, Metabase API REST (PUT `/api/dashboard/{id}` com dashcards)

---

## Mapa de Arquivos

| Arquivo | Ação | Responsabilidade |
|---|---|---|
| `sql/008_create_schema_registry.sql` | Criar | Tabela `tb_schema_registry` |
| `bot/schema_registry.py` | Criar | Match, registro, criação dinâmica de tabelas/SP |
| `bot/dashboard_builder.py` | Criar | Criação automática de dashboards no Metabase |
| `bot/database.py` | Modificar | Adicionar `obter_db_config()` com fallback ao registry |
| `bot/etl.py` | Modificar | Integrar auto-discovery após falha de prefixo |
| `scripts/migrar_schema_registry.py` | Criar | Seed inicial do registry com schema existente |

---

### Task 1: SQL — Tabela tb_schema_registry

**Files:**
- Create: `sql/008_create_schema_registry.sql`

- [ ] **Step 1: Criar o arquivo SQL**

```sql
-- sql/008_create_schema_registry.sql
USE [HML-DBFLOWETL01];
GO

IF OBJECT_ID('dbo.tb_schema_registry', 'U') IS NULL
CREATE TABLE dbo.tb_schema_registry (
    id_schema       INT IDENTITY(1,1) NOT NULL,
    nm_tabela       VARCHAR(200)      NOT NULL,
    nm_staging      VARCHAR(200)      NOT NULL,
    nm_sp_merge     VARCHAR(200)      NOT NULL,
    nm_chave        VARCHAR(100)      NOT NULL,
    colunas_base    VARCHAR(MAX)      NOT NULL,  -- JSON array ordenado de colunas
    id_dashboard_mb INT               NULL,
    nm_dashboard_mb VARCHAR(200)      NULL,
    dt_criacao      DATETIME          NOT NULL DEFAULT GETDATE(),
    dt_atualizacao  DATETIME          NULL,
    CONSTRAINT PK_tb_schema_registry  PRIMARY KEY (id_schema),
    CONSTRAINT UQ_schema_registry_tab UNIQUE (nm_tabela)
);
GO
```

- [ ] **Step 2: Executar no SQL Server**

```bash
docker exec flowetl-sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "102030@T" -No \
  -i /dev/stdin <<'EOF'
USE [HML-DBFLOWETL01];
IF OBJECT_ID('dbo.tb_schema_registry', 'U') IS NULL
CREATE TABLE dbo.tb_schema_registry (
    id_schema       INT IDENTITY(1,1) NOT NULL,
    nm_tabela       VARCHAR(200)      NOT NULL,
    nm_staging      VARCHAR(200)      NOT NULL,
    nm_sp_merge     VARCHAR(200)      NOT NULL,
    nm_chave        VARCHAR(100)      NOT NULL,
    colunas_base    VARCHAR(MAX)      NOT NULL,
    id_dashboard_mb INT               NULL,
    nm_dashboard_mb VARCHAR(200)      NULL,
    dt_criacao      DATETIME          NOT NULL DEFAULT GETDATE(),
    dt_atualizacao  DATETIME          NULL,
    CONSTRAINT PK_tb_schema_registry  PRIMARY KEY (id_schema),
    CONSTRAINT UQ_schema_registry_tab UNIQUE (nm_tabela)
);
EOF
```

Expected: zero output (sem erros).

- [ ] **Step 3: Verificar**

```bash
python -c "
from bot.database import obter_engine
from sqlalchemy import text
e = obter_engine()
with e.connect() as c:
    print(c.execute(text(\"SELECT OBJECT_ID('dbo.tb_schema_registry')\")).scalar())
"
```

Expected: qualquer número inteiro (não None).

- [ ] **Step 4: Commit**

```bash
git add sql/008_create_schema_registry.sql
git commit -m "feat: cria tb_schema_registry para auto-descoberta de schema"
```

---

### Task 2: bot/schema_registry.py — Match e Registro

**Files:**
- Create: `bot/schema_registry.py`

- [ ] **Step 1: Criar o arquivo**

```python
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
```

- [ ] **Step 2: Testar match e registro**

```bash
python -c "
from bot.schema_registry import registrar, buscar_match, buscar_config_por_tabela

# Registrar schema de teste
registrar('tb_teste_plan', 'stg_teste_plan', 'sp_merge_teste_plan', 'id',
          ['id', 'nome', 'status', 'prioridade'])

# Match exato
m = buscar_match(['id', 'nome', 'status', 'prioridade', 'nm_arquivo_origem'])
assert m is not None and m['nm_tabela'] == 'tb_teste_plan', f'Esperado match, got {m}'
print('match exato: OK')

# Match parcial >= 80%: 3/4 = 75% → abaixo do threshold
m2 = buscar_match(['id', 'nome', 'status'])
assert m2 is None, f'Esperado None para 75%, got {m2}'
print('match parcial 75% → None: OK')

# Config lookup
cfg = buscar_config_por_tabela('tb_teste_plan')
assert cfg == {'staging': 'stg_teste_plan', 'sp_merge': 'sp_merge_teste_plan'}
print('config lookup: OK')

# Limpar
from bot.database import obter_engine
from sqlalchemy import text
with obter_engine().begin() as c:
    c.execute(text(\"DELETE FROM dbo.tb_schema_registry WHERE nm_tabela = 'tb_teste_plan'\"))
print('limpeza: OK')
"
```

Expected: três linhas `OK`.

- [ ] **Step 3: Commit**

```bash
git add bot/schema_registry.py
git commit -m "feat: schema_registry com buscar_match, registrar e buscar_config_por_tabela"
```

---

### Task 3: bot/schema_registry.py — Criação Dinâmica de Pipeline

**Files:**
- Modify: `bot/schema_registry.py` (adicionar funções ao final)

- [ ] **Step 1: Adicionar as funções de geração de nomes e criação de tabelas**

Adicionar ao final de `bot/schema_registry.py`:

```python
# ------------------------------------------------------------------ pipeline dinâmico

def _gerar_nomes(nm_arquivo: str) -> tuple:
    """Deriva (nm_tabela, nm_staging, nm_sp) do nome do arquivo."""
    stem = Path(nm_arquivo).stem
    base = re.sub(r'[^a-z0-9]', '_', stem.lower())
    base = re.sub(r'_+', '_', base).strip('_')[:40]
    return f'tb_{base}', f'stg_{base}', f'sp_merge_{base}'


def _detectar_chave(colunas: list) -> str:
    """Heurística para detectar coluna de chave primária."""
    candidatos = {'id', 'numero', 'codigo', 'code', 'num', 'protocolo', 'chave'}
    for c in colunas:
        if c in candidatos or c.startswith('id_') or c.endswith('_id') or c.endswith('_num'):
            return c
    return colunas[0]


def criar_pipeline_novo(nm_arquivo: str, colunas: list) -> dict:
    """
    Cria tabelas staging + produção + SP MERGE para um schema desconhecido.
    Registra no tb_schema_registry.
    Retorna {'nm_tabela', 'nm_staging', 'nm_sp_merge', 'nm_chave'}.
    """
    from bot.database import _reconstruir_sp_merge

    colunas_dados = [c for c in colunas if c not in _IGNORAR_COLUNAS]
    nm_tabela, nm_staging, nm_sp = _gerar_nomes(nm_arquivo)

    # Resolver conflito de nome se tabela já existe
    engine = obter_engine()
    sufixo = 1
    nm_base = nm_tabela
    with engine.connect() as conn:
        while conn.execute(
            text("SELECT OBJECT_ID(:t)"), {'t': f'dbo.{nm_tabela}'}
        ).scalar() is not None:
            nm_tabela = f'{nm_base}_{sufixo}'
            nm_staging = f'stg_{nm_base[3:]}_{sufixo}'
            nm_sp = f'sp_merge_{nm_base[3:]}_{sufixo}'
            sufixo += 1

    for valor, campo in [(nm_tabela, 'nm_tabela'), (nm_staging, 'nm_staging'),
                         (nm_sp, 'nm_sp')]:
        _validar_identificador(valor, campo)

    chave = _detectar_chave(colunas_dados)
    colunas_sem_chave = [c for c in colunas_dados if c != chave]

    col_defs = ',\n    '.join(
        f'[{c}] VARCHAR(MAX) NULL' for c in colunas_sem_chave + ['nm_arquivo_origem']
    )

    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE dbo.{nm_staging} (
                [{chave}] VARCHAR(500) NULL,
                {col_defs}
            )
        """))
        conn.execute(text(f"""
            CREATE TABLE dbo.{nm_tabela} (
                [{chave}]      VARCHAR(500) NOT NULL,
                {col_defs},
                dt_insert      DATETIME NOT NULL DEFAULT GETDATE(),
                dt_atualizacao DATETIME NULL,
                CONSTRAINT PK_{nm_tabela} PRIMARY KEY ([{chave}])
            )
        """))

    logger.info(f'Tabelas criadas: {nm_staging}, {nm_tabela}')
    _reconstruir_sp_merge(nm_staging, nm_tabela, nm_sp, chave)

    registrar(nm_tabela, nm_staging, nm_sp, chave, colunas_dados)

    return {
        'nm_tabela':   nm_tabela,
        'nm_staging':  nm_staging,
        'nm_sp_merge': nm_sp,
        'nm_chave':    chave,
    }
```

- [ ] **Step 2: Testar criação de pipeline**

```bash
python -c "
from bot.schema_registry import criar_pipeline_novo
from bot.database import obter_engine
from sqlalchemy import text

colunas = ['numero', 'titulo', 'status', 'prioridade', 'nm_arquivo_origem']
resultado = criar_pipeline_novo('vendas_teste_2026.xlsx', colunas)
print('Pipeline criado:', resultado)

e = obter_engine()
with e.connect() as c:
    tab = c.execute(text(\"SELECT OBJECT_ID('dbo.' + :t)\"), {'t': resultado['nm_tabela']}).scalar()
    stg = c.execute(text(\"SELECT OBJECT_ID('dbo.' + :t)\"), {'t': resultado['nm_staging']}).scalar()
    sp  = c.execute(text(\"SELECT OBJECT_ID('dbo.' + :t + '', 'P')\"), {'t': resultado['nm_sp_merge']}).scalar()
    print('tabela existe:', tab is not None)
    print('staging existe:', stg is not None)
    print('SP existe:', sp is not None)

# Limpar
with e.begin() as c:
    sp_nm = resultado['nm_sp_merge']
    stg_nm = resultado['nm_staging']
    tab_nm = resultado['nm_tabela']
    c.execute(text(f\"IF OBJECT_ID('dbo.{sp_nm}','P') IS NOT NULL DROP PROCEDURE dbo.{sp_nm}\"))
    c.execute(text(f\"IF OBJECT_ID('dbo.{stg_nm}','U') IS NOT NULL DROP TABLE dbo.{stg_nm}\"))
    c.execute(text(f\"IF OBJECT_ID('dbo.{tab_nm}','U') IS NOT NULL DROP TABLE dbo.{tab_nm}\"))
    c.execute(text(\"DELETE FROM dbo.tb_schema_registry WHERE nm_tabela = :t\"), {'t': tab_nm})
print('limpeza: OK')
"
```

Expected: `tabela existe: True`, `staging existe: True`, `SP existe: True`, `limpeza: OK`.

- [ ] **Step 3: Commit**

```bash
git add bot/schema_registry.py
git commit -m "feat: schema_registry criar_pipeline_novo com DDL dinâmico e SP MERGE"
```

---

### Task 4: bot/dashboard_builder.py — Dashboard Automático no Metabase

**Files:**
- Create: `bot/dashboard_builder.py`

- [ ] **Step 1: Criar o arquivo**

```python
"""
Criação automática de dashboards no Metabase baseada nas colunas da tabela.
Detecta padrões de colunas e gera cards apropriados (bar, pie, scalar).
"""

import os

import requests
import urllib3
from dotenv import load_dotenv

from bot.logger import configurar_logger

load_dotenv(override=True)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = configurar_logger(__name__)

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
                row, col_pos, size_x, size_y = 0, 0, 18, 4
            else:
                j = i - 1
                row     = 4 + (j // 2) * 6
                col_pos = (j % 2) * 9
                size_x  = 9
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
```

- [ ] **Step 2: Testar criação de dashboard para tabela existente**

```bash
python -c "
from bot.dashboard_builder import criar_dashboard_automatico

# Usa tb_problemas_gov_ti que já existe no banco
colunas = ['numero','titulo','status','prioridade','gerente_responsavel',
           'departamento_relator','sistema','paliativo','jornada_impactada']

dash_id, nm_dash = criar_dashboard_automatico('tb_problemas_gov_ti_teste', colunas)
print(f'dash_id={dash_id}, nome={nm_dash}')
assert dash_id > 0, f'Esperado dash_id > 0, got {dash_id}'
print('Dashboard criado com sucesso')

# Limpar dashboard de teste via API
import requests, os
from dotenv import load_dotenv
load_dotenv(override=True)
token = requests.post('http://172.18.0.2:3000/api/session',
    json={'username': os.getenv('MB_ADMIN_USER'), 'password': os.getenv('MB_ADMIN_PASS')},
    verify=False, timeout=10).json()['id']
requests.delete(f'http://172.18.0.2:3000/api/dashboard/{dash_id}',
    headers={'X-Metabase-Session': token}, verify=False)
print('limpeza: OK')
"
```

Expected: `dash_id=<numero positivo>`, `Dashboard criado com sucesso`, `limpeza: OK`.

- [ ] **Step 3: Commit**

```bash
git add bot/dashboard_builder.py
git commit -m "feat: dashboard_builder cria dashboard Metabase automaticamente por padrão de colunas"
```

---

### Task 5: Integrar Auto-Discovery no ETL

**Files:**
- Modify: `bot/database.py` (adicionar `obter_db_config`)
- Modify: `bot/etl.py` (integrar auto-discovery)

- [ ] **Step 1: Adicionar `obter_db_config` em bot/database.py**

Adicionar após a definição de `TABELA_CONFIG` (linha ~204 de `bot/database.py`):

```python
def obter_db_config(nm_tabela: str) -> Optional[dict]:
    """Retorna {'staging', 'sp_merge'} verificando TABELA_CONFIG e depois schema_registry."""
    if nm_tabela in TABELA_CONFIG:
        return TABELA_CONFIG[nm_tabela]
    try:
        from bot.schema_registry import buscar_config_por_tabela
        return buscar_config_por_tabela(nm_tabela)
    except Exception as exc:
        logger.error(f'obter_db_config: erro ao consultar schema_registry: {exc}')
    return None
```

- [ ] **Step 2: Modificar bot/etl.py — substituir bloco de identificação**

Localizar e substituir o bloco de `_identificar_prefixo` até `db_config = TABELA_CONFIG.get(nm_tabela)` (linhas ~111–129) pelo seguinte:

```python
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
        cfg_limpeza = {}   # sem renomear/ignorar colunas — só sanitização
        chave       = None  # detectado após ler o arquivo
        nm_tabela   = None
        modo_discovery = True
```

- [ ] **Step 3: Modificar bot/etl.py — bloco try principal**

Localizar o bloco `try:` principal (a partir da linha `# 1-2. Ler xlsx na aba correta`).

Substituir o bloco de `sincronizar_colunas` e adiante para incluir a resolução de `nm_tabela` no caminho discovery. O bloco completo do `try` passa a ser:

```python
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
            from bot.database import obter_db_config
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
```

- [ ] **Step 4: Remover o import de TABELA_CONFIG que não é mais usado no topo do etl.py**

Localizar a linha (topo de `bot/etl.py`):
```python
from bot.database import TABELA_CONFIG, carregar_staging, executar_merge, obter_engine, sincronizar_colunas
```

Substituir por:
```python
from bot.database import carregar_staging, executar_merge, obter_db_config, obter_engine, sincronizar_colunas
```

- [ ] **Step 5: Testar caminho 1 (tabelas.json) ainda funciona**

```bash
cp "processados/2026-05/GProblemas_GOV TI.xlsx" "pasta_monitorada/GProblemas_GOV TI.xlsx"
python -c "
from bot.etl import processar_arquivo
ok = processar_arquivo('pasta_monitorada/GProblemas_GOV TI.xlsx', skip_retry=True)
print('resultado:', ok)
assert ok is True
print('caminho tabelas.json: OK')
"
```

Expected: `caminho tabelas.json: OK`.

- [ ] **Step 6: Testar caminho 2 — schema match (arquivo sem prefixo, colunas conhecidas)**

```bash
python -c "
# Seed do registry com schema existente (será feito formalmente na Task 6,
# mas aqui precisamos do registro para testar o match)
from bot.schema_registry import registrar
from bot.database import obter_engine
from sqlalchemy import text
e = obter_engine()
with e.connect() as c:
    cols = [r[0] for r in c.execute(text('''
        SELECT LOWER(name) FROM sys.columns
        WHERE object_id = OBJECT_ID(''dbo.tb_problemas_gov_ti'')
          AND name NOT IN (''dt_insert'',''dt_atualizacao'')
        ORDER BY column_id
    ''')).fetchall()]
registrar('tb_problemas_gov_ti','stg_problemas_gov_ti','sp_merge_problemas_gov_ti','numero', cols)
print('seed: OK')
"
```

```bash
import shutil, pathlib
shutil.copy('processados/2026-05/GProblemas_GOV TI.xlsx',
            'pasta_monitorada/sem_prefixo_2026.xlsx')
python -c "
from bot.etl import processar_arquivo
ok = processar_arquivo('pasta_monitorada/sem_prefixo_2026.xlsx', skip_retry=True)
print('resultado:', ok)
assert ok is True
print('caminho discovery match: OK')
"
```

Expected: log mostrando `Auto-discovery: match com tb_problemas_gov_ti`, `resultado: True`.

- [ ] **Step 7: Testar caminho 3 — schema novo (cria pipeline + dashboard)**

Criar arquivo Excel com schema totalmente diferente:

```bash
python -c "
import pandas as pd, pathlib
df = pd.DataFrame([
    {'protocolo': '001', 'solicitante': 'Ana', 'categoria': 'Hardware', 'urgencia': 'Alta', 'descricao': 'Troca de HD'},
    {'protocolo': '002', 'solicitante': 'Bruno', 'categoria': 'Software', 'urgencia': 'Baixa', 'descricao': 'Instalação'},
])
df.to_excel('pasta_monitorada/chamados_ti_2026.xlsx', index=False)
print('arquivo criado')
"
python -c "
from bot.etl import processar_arquivo
ok = processar_arquivo('pasta_monitorada/chamados_ti_2026.xlsx', skip_retry=True)
print('resultado:', ok)
assert ok is True
print('caminho discovery novo pipeline: OK')
"
```

Expected: log com `Auto-discovery: schema novo — criando pipeline`, tabelas criadas, dashboard criado, `resultado: True`.

- [ ] **Step 8: Commit**

```bash
git add bot/database.py bot/etl.py
git commit -m "feat: integra auto-discovery de schema no ETL com fallback para tabelas.json"
```

---

### Task 6: Migration — Seed do Registry + Limpeza

**Files:**
- Create: `scripts/migrar_schema_registry.py`

- [ ] **Step 1: Criar o script de migração**

```python
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
```

- [ ] **Step 2: Executar a migração**

```bash
python scripts/migrar_schema_registry.py
```

Expected:
```
[...] INFO  migrar_schema_registry — tb_problemas_gov_ti: N colunas registradas.
Migração concluída.
```

- [ ] **Step 3: Verificar registry**

```bash
python -c "
from bot.database import obter_engine
from sqlalchemy import text
import json
e = obter_engine()
with e.connect() as c:
    rows = c.execute(text('SELECT nm_tabela, nm_chave, colunas_base FROM dbo.tb_schema_registry')).fetchall()
for r in rows:
    cols = json.loads(r[2])
    print(f'{r[0]} | chave={r[1]} | {len(cols)} colunas')
"
```

Expected: uma linha por tabela registrada com contagem de colunas.

- [ ] **Step 4: Limpar tabelas de teste criadas na Task 5**

```bash
python -c "
from bot.database import obter_engine
from sqlalchemy import text
e = obter_engine()
with e.connect() as c:
    rows = c.execute(text(\"\"\"
        SELECT nm_tabela, nm_staging, nm_sp_merge
        FROM dbo.tb_schema_registry
        WHERE nm_tabela NOT IN ('tb_problemas_gov_ti')
    \"\"\")).fetchall()

with e.begin() as c:
    for r in rows:
        c.execute(text(f\"IF OBJECT_ID('dbo.{r[2]}','P') IS NOT NULL DROP PROCEDURE dbo.{r[2]}\"))
        c.execute(text(f\"IF OBJECT_ID('dbo.{r[1]}','U') IS NOT NULL DROP TABLE dbo.{r[1]}\"))
        c.execute(text(f\"IF OBJECT_ID('dbo.{r[0]}','U') IS NOT NULL DROP TABLE dbo.{r[0]}\"))
        c.execute(text(\"DELETE FROM dbo.tb_schema_registry WHERE nm_tabela = :t\"), {'t': r[0]})
        print(f'removido: {r[0]}')
"
```

- [ ] **Step 5: Commit final**

```bash
git add scripts/migrar_schema_registry.py
git commit -m "feat: script de migração seed do schema_registry + auto-discovery completo"
```

---

## Self-Review

### Cobertura do spec
| Requisito | Task |
|---|---|
| tabelas.json como override (prioridade 1) | Task 5 |
| Schema fingerprint com threshold 80% | Task 2 |
| Match por colunas (novas colunas = ainda match) | Task 2 (`sincronizar_colunas` cuida das extras) |
| Schema totalmente diferente → novo pipeline | Task 3 |
| Criação automática de tabelas SQL + SP MERGE | Task 3 |
| Dashboard Metabase auto-gerado | Task 4 |
| Cards inferidos por padrão de colunas | Task 4 |
| Seed do registry com dados existentes | Task 6 |

### Pontos de atenção
- A **chave detectada automaticamente** (`_detectar_chave`) usa heurística. Se a planilha nova tiver uma chave com nome não óbvio, a coluna `colunas[0]` é usada como fallback — pode precisar de ajuste manual posterior via `UPDATE tb_schema_registry`.
- O **tipo da chave** em tabelas dinâmicas é `VARCHAR(500)`. Se a chave for numérica (ex: INTEGER), o MERGE funciona mas há overhead de comparação. Pode-se alterar manualmente após criação.
- O `criar_dashboard_automatico` **não é crítico** — falha nele não aborta o ETL (o arquivo é processado mesmo que o dashboard falhe).
