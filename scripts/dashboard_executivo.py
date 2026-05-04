"""
Cria o Dashboard Executivo de Governança TI no Metabase via API.

Cards criados (10):
  1. Scorecard — Total de Problemas
  2. Scorecard — Problemas Abertos
  3. Scorecard — Críticos P1 em Aberto
  4. Problemas por Status           (barra)
  5. Problemas por Prioridade       (pizza)
  6. Problemas por Jornada Impactada (barra horizontal)
  7. Paliativo — Sim vs Não         (donut)
  8. Problemas por Gerente          (barra horizontal)
  9. Problemas por Departamento     (barra horizontal)
 10. Tabela: Problemas Abertos      (tabela completa)

Uso:
    python scripts/dashboard_executivo.py
"""

import os
import sys
import time
from pathlib import Path

import requests
import urllib3
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE        = "https://localhost"
ADMIN_EMAIL = "pcs@tf.com.br"
ADMIN_SENHA = os.getenv("MSSQL_SA_PASSWORD", "")
DB_HOST     = "sqlserver"
DB_PORT     = 1433
DB_NAME     = "HML-DBFLOWETL01"
DB_USER     = "sa"
DB_PASS     = os.getenv("MSSQL_SA_PASSWORD", "")
DASH_NAME   = "Governança TI — Dashboard Executivo"


# ── helpers ──────────────────────────────────────────────────────────────────

def get(path, headers=None):
    return requests.get(f"{BASE}{path}", headers=headers, verify=False)

def post(path, payload, headers=None):
    return requests.post(f"{BASE}{path}", json=payload, headers=headers, verify=False)

def put(path, payload, headers=None):
    return requests.put(f"{BASE}{path}", json=payload, headers=headers, verify=False)

def step(msg):
    print(f"\n{'─'*55}\n▶  {msg}")


# ── 1. Login ──────────────────────────────────────────────────────────────────
step("Login no Metabase")
r = post("/api/session", {"username": ADMIN_EMAIL, "password": ADMIN_SENHA})
if r.status_code != 200:
    print(f"Erro no login: {r.text}")
    sys.exit(1)
session_id = r.json()["id"]
HDR = {"X-Metabase-Session": session_id, "Content-Type": "application/json"}
print("   OK")


# ── 2. Banco de dados ─────────────────────────────────────────────────────────
step("Obtendo ID do banco SQL Server")
r = get("/api/database", HDR)
dbs = r.json().get("data", r.json() if isinstance(r.json(), list) else [])
db_row = next((d for d in dbs if d.get("name") == "FlowETL - SQL Server"), None)

if db_row:
    db_id = db_row["id"]
    print(f"   Banco encontrado (ID {db_id})")
else:
    step("Registrando SQL Server no Metabase")
    payload = {
        "name": "FlowETL - SQL Server",
        "engine": "sqlserver",
        "details": {
            "host": DB_HOST, "port": DB_PORT, "db": DB_NAME,
            "user": DB_USER, "password": DB_PASS,
            "ssl": False, "tunnel-enabled": False,
            "additional-options": "trustServerCertificate=true",
        },
        "auto_run_queries": True, "is_full_sync": True,
    }
    r = post("/api/database", payload, HDR)
    if r.status_code not in (200, 201):
        print(f"Erro ao conectar banco: {r.text}")
        sys.exit(1)
    db_id = r.json()["id"]
    print(f"   SQL Server registrado (ID {db_id}). Aguardando sync...")
    time.sleep(20)


# ── 3. Definição dos cards ────────────────────────────────────────────────────
CARDS = [
    # ── Scorecards
    {
        "name": "[Exec] Total de Problemas",
        "query": "SELECT COUNT(*) AS total FROM dbo.tb_problemas_gov_ti",
        "display": "scalar",
        "viz": {},
    },
    {
        "name": "[Exec] Problemas Abertos",
        "query": (
            "SELECT COUNT(*) AS abertos FROM dbo.tb_problemas_gov_ti "
            "WHERE status NOT IN ('Resolvido', 'Cancelado')"
        ),
        "display": "scalar",
        "viz": {},
    },
    {
        "name": "[Exec] Críticos P1 em Aberto",
        "query": (
            "SELECT COUNT(*) AS p1_abertos FROM dbo.tb_problemas_gov_ti "
            "WHERE prioridade = 'P1' AND status NOT IN ('Resolvido', 'Cancelado')"
        ),
        "display": "scalar",
        "viz": {},
    },
    # ── Gráficos
    {
        "name": "[Exec] Problemas por Status",
        "query": (
            "SELECT status, COUNT(*) AS qt_problemas "
            "FROM dbo.tb_problemas_gov_ti "
            "GROUP BY status ORDER BY qt_problemas DESC"
        ),
        "display": "bar",
        "viz": {
            "graph.dimensions": ["status"],
            "graph.metrics": ["qt_problemas"],
            "graph.x_axis.title_text": "Status",
            "graph.y_axis.title_text": "Quantidade",
            "graph.label_value_formatting": "auto",
        },
    },
    {
        "name": "[Exec] Problemas por Prioridade",
        "query": (
            "SELECT prioridade, COUNT(*) AS qt_problemas "
            "FROM dbo.tb_problemas_gov_ti "
            "GROUP BY prioridade ORDER BY prioridade"
        ),
        "display": "pie",
        "viz": {
            "pie.dimension": "prioridade",
            "pie.metric": "qt_problemas",
            "pie.show_legend": True,
        },
    },
    {
        "name": "[Exec] Problemas por Jornada Impactada",
        "query": (
            "SELECT ISNULL(jornada_impactada,'Não informado') AS jornada_impactada, "
            "COUNT(*) AS qt_problemas "
            "FROM dbo.tb_problemas_gov_ti "
            "GROUP BY jornada_impactada ORDER BY qt_problemas DESC"
        ),
        "display": "bar",
        "viz": {
            "graph.dimensions": ["jornada_impactada"],
            "graph.metrics": ["qt_problemas"],
            "graph.x_axis.title_text": "Jornada",
            "graph.y_axis.title_text": "Quantidade",
        },
    },
    {
        "name": "[Exec] Paliativo — Sim vs Não",
        "query": (
            "SELECT ISNULL(paliativo,'Não informado') AS paliativo, "
            "COUNT(*) AS qt_problemas "
            "FROM dbo.tb_problemas_gov_ti "
            "GROUP BY paliativo"
        ),
        "display": "pie",
        "viz": {
            "pie.dimension": "paliativo",
            "pie.metric": "qt_problemas",
            "pie.show_legend": True,
        },
    },
    {
        "name": "[Exec] Problemas por Gerente Responsável",
        "query": (
            "SELECT ISNULL(gerente_responsavel,'Não informado') AS gerente_responsavel, "
            "COUNT(*) AS qt_problemas "
            "FROM dbo.tb_problemas_gov_ti "
            "GROUP BY gerente_responsavel ORDER BY qt_problemas DESC"
        ),
        "display": "bar",
        "viz": {
            "graph.dimensions": ["gerente_responsavel"],
            "graph.metrics": ["qt_problemas"],
            "graph.x_axis.title_text": "Gerente",
            "graph.y_axis.title_text": "Quantidade",
        },
    },
    {
        "name": "[Exec] Problemas por Departamento Relator",
        "query": (
            "SELECT ISNULL(departamento_relator,'Não informado') AS departamento_relator, "
            "COUNT(*) AS qt_problemas "
            "FROM dbo.tb_problemas_gov_ti "
            "GROUP BY departamento_relator ORDER BY qt_problemas DESC"
        ),
        "display": "bar",
        "viz": {
            "graph.dimensions": ["departamento_relator"],
            "graph.metrics": ["qt_problemas"],
            "graph.x_axis.title_text": "Departamento",
            "graph.y_axis.title_text": "Quantidade",
        },
    },
    # ── Tabela detalhada
    {
        "name": "[Exec] Tabela: Problemas Abertos",
        "query": (
            "SELECT "
            "  numero, prioridade, titulo, status, "
            "  CONVERT(VARCHAR, dt_abertura, 103) AS dt_abertura, "
            "  ISNULL(jornada_impactada,'—') AS jornada_impactada, "
            "  ISNULL(gerente_responsavel,'—') AS gerente_responsavel, "
            "  ISNULL(departamento_relator,'—') AS departamento_relator, "
            "  ISNULL(paliativo,'—') AS paliativo "
            "FROM dbo.tb_problemas_gov_ti "
            "WHERE status NOT IN ('Resolvido','Cancelado') "
            "ORDER BY "
            "  CASE prioridade WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END, "
            "  dt_abertura"
        ),
        "display": "table",
        "viz": {
            "table.pivot": False,
            "column_settings": {
                '["name","prioridade"]': {"column_title": "Prioridade"},
                '["name","jornada_impactada"]': {"column_title": "Jornada Impactada"},
                '["name","gerente_responsavel"]': {"column_title": "Gerente Responsável"},
                '["name","departamento_relator"]': {"column_title": "Departamento Relator"},
                '["name","paliativo"]': {"column_title": "Paliativo"},
            },
        },
    },
]


# ── 4. Criar ou reutilizar cards ──────────────────────────────────────────────
step("Criando cards KPI")

r = get("/api/card", HDR)
cards_existentes = r.json() if isinstance(r.json(), list) else r.json().get("data", [])
existentes_por_nome = {c["name"]: c["id"] for c in cards_existentes}

card_ids = []
for card in CARDS:
    if card["name"] in existentes_por_nome:
        cid = existentes_por_nome[card["name"]]
        print(f"   [existe]  {card['name']} (ID {cid})")
        card_ids.append(cid)
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
        "collection_id": None,
    }
    r = post("/api/card", payload, HDR)
    if r.status_code not in (200, 201):
        print(f"   [erro]    {card['name']}: {r.text[:200]}")
        card_ids.append(None)
        continue
    cid = r.json()["id"]
    card_ids.append(cid)
    print(f"   [criado]  {card['name']} (ID {cid})")


# ── 5. Dashboard ──────────────────────────────────────────────────────────────
step(f"Criando dashboard: {DASH_NAME}")

r = get("/api/dashboard", HDR)
dashboards = r.json() if isinstance(r.json(), list) else r.json().get("data", [])
dash_row = next((d for d in dashboards if d.get("name") == DASH_NAME), None)

if dash_row:
    dash_id = dash_row["id"]
    print(f"   Dashboard já existe (ID {dash_id}) — atualizando layout...")
else:
    r = post("/api/dashboard", {"name": DASH_NAME, "collection_id": None}, HDR)
    if r.status_code not in (200, 201):
        print(f"Erro ao criar dashboard: {r.text}")
        sys.exit(1)
    dash_id = r.json()["id"]
    print(f"   Dashboard criado (ID {dash_id})")

# Layout (grid 24 colunas)
# Linha 0: 3 scorecards (8 colunas cada, altura 4)
# Linha 4: Status (12) | Prioridade (12), altura 8
# Linha 12: Jornada (12) | Paliativo (12), altura 8
# Linha 20: Gerente (12) | Departamento (12), altura 8
# Linha 28: Tabela completa (24), altura 10
LAYOUT = [
    {"row": 0,  "col": 0,  "size_x": 8,  "size_y": 4},   # 0 Total
    {"row": 0,  "col": 8,  "size_x": 8,  "size_y": 4},   # 1 Abertos
    {"row": 0,  "col": 16, "size_x": 8,  "size_y": 4},   # 2 P1 críticos
    {"row": 4,  "col": 0,  "size_x": 12, "size_y": 8},   # 3 Status
    {"row": 4,  "col": 12, "size_x": 12, "size_y": 8},   # 4 Prioridade
    {"row": 12, "col": 0,  "size_x": 12, "size_y": 8},   # 5 Jornada
    {"row": 12, "col": 12, "size_x": 12, "size_y": 8},   # 6 Paliativo
    {"row": 20, "col": 0,  "size_x": 12, "size_y": 8},   # 7 Gerente
    {"row": 20, "col": 12, "size_x": 12, "size_y": 8},   # 8 Departamento
    {"row": 28, "col": 0,  "size_x": 24, "size_y": 10},  # 9 Tabela
]

dashcards = [
    {
        "id": -(i + 1),
        "card_id": cid,
        "row": pos["row"],
        "col": pos["col"],
        "size_x": pos["size_x"],
        "size_y": pos["size_y"],
        "parameter_mappings": [],
        "visualization_settings": {},
    }
    for i, (cid, pos) in enumerate(zip(card_ids, LAYOUT))
    if cid is not None
]

r = put(f"/api/dashboard/{dash_id}", {"dashcards": dashcards}, HDR)
if r.status_code not in (200, 201):
    print(f"   [erro] ao montar dashboard: {r.text[:300]}")
    sys.exit(1)

print(f"   {len(dashcards)} cards posicionados no dashboard.")


# ── Resumo ────────────────────────────────────────────────────────────────────
print(f"""
{'='*55}
Dashboard Executivo criado com sucesso!

  URL:       https://localhost/dashboard/{dash_id}
  Email:     {ADMIN_EMAIL}
  Dashboard: {DASH_NAME}
  Cards:     {len(dashcards)}

Layout:
  Linha 1  — Scorecards: Total | Abertos | P1 Críticos
  Linha 2  — Status (barra) | Prioridade (pizza)
  Linha 3  — Jornada Impactada (barra) | Paliativo (donut)
  Linha 4  — Gerente (barra) | Departamento (barra)
  Linha 5  — Tabela completa: Problemas Abertos
{'='*55}
""")
