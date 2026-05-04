"""
Configura o Metabase via API:
  1. Setup inicial (admin + preferências)
  2. Conexão com SQL Server
  3. Cria 7 cards KPI
  4. Cria dashboard "FlowETL — Governança de Problemas TI"
"""

import os
import sys
import time

import requests
import urllib3
from dotenv import load_dotenv

load_dotenv()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE        = "https://localhost"
ADMIN_EMAIL = "pcs@tf.com.br"
ADMIN_SENHA = os.getenv("MSSQL_SA_PASSWORD", "")
DB_HOST     = "sqlserver"          # nome do serviço no Docker
DB_PORT     = 1433
DB_NAME     = "HML-DBFLOWETL01"
DB_USER     = "sa"
DB_PASS     = os.getenv("MSSQL_SA_PASSWORD", "")

CARDS = [
    {
        "name": "Problemas por Status",
        "query": "SELECT status, COUNT(*) AS qt_problemas FROM dbo.tb_problemas_gov_ti GROUP BY status ORDER BY qt_problemas DESC",
        "display": "bar",
        "viz": {
            "graph.dimensions": ["status"],
            "graph.metrics": ["qt_problemas"],
            "graph.x_axis.title_text": "Status",
            "graph.y_axis.title_text": "Quantidade",
        },
    },
    {
        "name": "Problemas por Prioridade",
        "query": "SELECT prioridade, COUNT(*) AS qt_problemas FROM dbo.tb_problemas_gov_ti GROUP BY prioridade ORDER BY prioridade",
        "display": "pie",
        "viz": {
            "pie.dimension": "prioridade",
            "pie.metric": "qt_problemas",
        },
    },
    {
        "name": "Problemas por Gerente Responsável",
        "query": "SELECT gerente_responsavel, COUNT(*) AS qt_problemas FROM dbo.tb_problemas_gov_ti GROUP BY gerente_responsavel ORDER BY qt_problemas DESC",
        "display": "bar",
        "viz": {
            "graph.dimensions": ["gerente_responsavel"],
            "graph.metrics": ["qt_problemas"],
        },
    },
    {
        "name": "Problemas por Departamento",
        "query": "SELECT departamento_relator, COUNT(*) AS qt_problemas FROM dbo.tb_problemas_gov_ti GROUP BY departamento_relator ORDER BY qt_problemas DESC",
        "display": "bar",
        "viz": {
            "graph.dimensions": ["departamento_relator"],
            "graph.metrics": ["qt_problemas"],
        },
    },
    {
        "name": "Problemas por Sistema",
        "query": "SELECT sistema, COUNT(*) AS qt_problemas FROM dbo.tb_problemas_gov_ti GROUP BY sistema ORDER BY qt_problemas DESC",
        "display": "bar",
        "viz": {
            "graph.dimensions": ["sistema"],
            "graph.metrics": ["qt_problemas"],
        },
    },
    {
        "name": "Problemas Abertos",
        "query": (
            "SELECT numero, prioridade, titulo, status, "
            "CONVERT(VARCHAR, dt_abertura, 103) AS dt_abertura, "
            "gerente_responsavel, departamento_relator, sistema "
            "FROM dbo.tb_problemas_gov_ti "
            "WHERE status NOT IN ('Resolvido', 'Cancelado') "
            "ORDER BY prioridade, dt_abertura"
        ),
        "display": "table",
        "viz": {},
    },
    {
        "name": "Pipeline Health — Execuções ETL",
        "query": (
            "SELECT CONVERT(VARCHAR, dt_execucao, 103) AS data, "
            "qt_execucoes, qt_sucesso, qt_falha, "
            "total_linhas_inseridas, ROUND(media_duracao_seg, 2) AS media_seg "
            "FROM dbo.vw_pipeline_health ORDER BY dt_execucao DESC"
        ),
        "display": "table",
        "viz": {},
    },
]


def get(path, headers=None):
    return requests.get(f"{BASE}{path}", headers=headers, verify=False)


def post(path, payload, headers=None):
    return requests.post(f"{BASE}{path}", json=payload, headers=headers, verify=False)


def step(msg):
    print(f"\n{'─'*50}\n{msg}")


# ── 1. Setup inicial ──────────────────────────────────────────────────────────
step("1. Verificando estado do Metabase...")
r = get("/api/session/properties")
props = r.json()
setup_token = props.get("setup-token")

if setup_token:
    print("Setup token encontrado. Tentando configurar Metabase...")
    payload = {
        "token": setup_token,
        "user": {
            "first_name": "FlowETL",
            "last_name": "Admin",
            "email": ADMIN_EMAIL,
            "password": ADMIN_SENHA,
            "site_name": "FlowETL",
        },
        "prefs": {
            "site_name": "FlowETL — Governança TI",
            "allow_tracking": False,
        },
    }
    r = post("/api/setup", payload)
    if r.status_code in (200, 201):
        session_id = r.json()["id"]
        print("Setup concluído.")
    else:
        print("Usuário já existe. Fazendo login...")
        r = post("/api/session", {"username": ADMIN_EMAIL, "password": ADMIN_SENHA})
        if r.status_code != 200:
            print(f"Erro no login: {r.text}")
            sys.exit(1)
        session_id = r.json()["id"]
else:
    print("Fazendo login...")
    r = post("/api/session", {"username": ADMIN_EMAIL, "password": ADMIN_SENHA})
    if r.status_code != 200:
        print(f"Erro no login: {r.text}")
        sys.exit(1)
    session_id = r.json()["id"]

headers = {"X-Metabase-Session": session_id, "Content-Type": "application/json"}
print(f"Sessão iniciada.")

# ── 2. Conexão com SQL Server ─────────────────────────────────────────────────
step("2. Conectando ao SQL Server...")

r = get("/api/database", headers)
databases = r.json().get("data", r.json() if isinstance(r.json(), list) else [])
db_existente = next((d for d in databases if d.get("name") == "FlowETL - SQL Server"), None)

if db_existente:
    db_id = db_existente["id"]
    print(f"Banco já conectado (ID {db_id}).")
else:
    payload = {
        "name": "FlowETL - SQL Server",
        "engine": "sqlserver",
        "details": {
            "host": DB_HOST,
            "port": DB_PORT,
            "db": DB_NAME,
            "user": DB_USER,
            "password": DB_PASS,
            "ssl": False,
            "tunnel-enabled": False,
            "additional-options": "trustServerCertificate=true",
        },
        "auto_run_queries": True,
        "is_full_sync": True,
    }
    r = post("/api/database", payload, headers)
    if r.status_code not in (200, 201):
        print(f"Erro ao conectar banco: {r.text}")
        sys.exit(1)
    db_id = r.json()["id"]
    print(f"SQL Server conectado (ID {db_id}). Aguardando sync...")
    time.sleep(15)

# ── 3. Criar cards KPI ────────────────────────────────────────────────────────
step("3. Criando cards KPI...")

# Buscar coleção raiz
r = get("/api/collection/root", headers)
collection_id = None  # None = coleção raiz (Our analytics)

card_ids = []
for card in CARDS:
    # Verifica se já existe
    r = get("/api/card", headers)
    cards_existentes = r.json() if isinstance(r.json(), list) else r.json().get("data", [])
    existente = next((c for c in cards_existentes if c.get("name") == card["name"]), None)
    if existente:
        print(f"  [existe] {card['name']}")
        card_ids.append(existente["id"])
        continue

    payload = {
        "name": card["name"],
        "dataset_query": {
            "type": "native",
            "native": {"query": card["query"]},
            "database": db_id,
        },
        "display": card["display"],
        "visualization_settings": card["viz"],
        "collection_id": collection_id,
    }
    r = post("/api/card", payload, headers)
    if r.status_code not in (200, 201):
        print(f"  [erro] {card['name']}: {r.text[:200]}")
        continue
    cid = r.json()["id"]
    card_ids.append(cid)
    print(f"  [criado] {card['name']} (ID {cid})")

# ── 4. Criar dashboard ────────────────────────────────────────────────────────
step("4. Criando dashboard...")

DASH_NAME = "FlowETL — Governança de Problemas TI"
r = get("/api/dashboard", headers)
dashboards = r.json() if isinstance(r.json(), list) else r.json().get("data", [])
dash_existente = next((d for d in dashboards if d.get("name") == DASH_NAME), None)

if dash_existente:
    dash_id = dash_existente["id"]
    print(f"Dashboard já existe (ID {dash_id}). Atualizando cards...")
else:
    r = post("/api/dashboard", {"name": DASH_NAME, "collection_id": collection_id}, headers)
    if r.status_code not in (200, 201):
        print(f"Erro ao criar dashboard: {r.text}")
        sys.exit(1)
    dash_id = r.json()["id"]
    print(f"Dashboard criado (ID {dash_id}).")

layout = [
    {"row": 0,  "col": 0,  "size_x": 12, "size_y": 8},
    {"row": 0,  "col": 12, "size_x": 12, "size_y": 8},
    {"row": 8,  "col": 0,  "size_x": 12, "size_y": 8},
    {"row": 8,  "col": 12, "size_x": 12, "size_y": 8},
    {"row": 16, "col": 0,  "size_x": 12, "size_y": 8},
    {"row": 16, "col": 12, "size_x": 12, "size_y": 8},
    {"row": 24, "col": 0,  "size_x": 24, "size_y": 8},
]

dashcards = [
    {
        "id": -(i + 1),
        "card_id": card_id,
        "row": pos["row"],
        "col": pos["col"],
        "size_x": pos["size_x"],
        "size_y": pos["size_y"],
        "parameter_mappings": [],
        "visualization_settings": {},
    }
    for i, (card_id, pos) in enumerate(zip(card_ids, layout))
]

r = requests.put(
    f"{BASE}/api/dashboard/{dash_id}",
    json={"dashcards": dashcards},
    headers=headers,
    verify=False,
)
if r.status_code not in (200, 201):
    print(f"  [erro] ao montar dashboard: {r.text[:300]}")
else:
    print(f"  {len(dashcards)} cards adicionados ao dashboard.")

# ── Resumo ────────────────────────────────────────────────────────────────────
print(f"""
{'='*50}
Metabase configurado com sucesso!

  URL:   https://localhost
  Email: {ADMIN_EMAIL}
  Senha: {ADMIN_SENHA}

  Dashboard: {DASH_NAME}
  Cards criados: {len(card_ids)}
{'='*50}
""")
