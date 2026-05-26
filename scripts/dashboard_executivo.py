"""
Cria (ou atualiza) o Dashboard Executivo de Governança TI no Metabase.

Cards criados (10):
  1–3. Scorecards: Total | Abertos | Críticos P1
  4.   Problemas por Status           (barra)
  5.   Problemas por Prioridade       (pizza)
  6.   Problemas por Jornada Impactada (barra)
  7.   Paliativo — Sim vs Não         (pizza)
  8.   Problemas por Gerente          (barra)
  9.   Problemas por Departamento     (barra)
  10.  Tabela: Problemas Abertos      (tabela)

Filtros em cascata (configurados automaticamente):
  Prioridade → Status → Departamento → Gerente Responsável
                     ↘ Jornada Impactada
                     ↘ Paliativo

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
load_dotenv(override=True)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE        = os.getenv("MB_SITE_URL", "https://localhost").rstrip("/")
ADMIN_EMAIL = os.getenv("MB_ADMIN_USER")
ADMIN_SENHA = os.getenv("MB_ADMIN_PASS")
DB_HOST     = os.getenv("DB_HOST", "sqlserver")
DB_PORT     = int(os.getenv("DB_PORT", "1433"))
DB_NAME     = os.getenv("DB_NAME", "")
DB_USER     = os.getenv("DB_USER", "sa")
DB_PASS     = os.getenv("MSSQL_SA_PASSWORD", "")
DASH_NAME   = "Governança TI — Dashboard Executivo"

if not ADMIN_EMAIL or not ADMIN_SENHA:
    print("❌ Erro: MB_ADMIN_USER e MB_ADMIN_PASS devem estar definidas no .env")
    sys.exit(1)


# ── helpers ───────────────────────────────────────────────────────────────────

def get(path, headers=None):
    return requests.get(f"{BASE}{path}", headers=headers, verify=False)

def post(path, payload, headers=None):
    return requests.post(f"{BASE}{path}", json=payload, headers=headers, verify=False)

def put(path, payload, headers=None):
    return requests.put(f"{BASE}{path}", json=payload, headers=headers, verify=False)

def step(msg):
    print(f"\n{'─'*55}\n▶  {msg}")


# ── Definição dos filtros em cascata ──────────────────────────────────────────
# filteringParameters: IDs dos filtros que restringem as opções deste.
# Prioridade é o topo — não depende de ninguém.
# Gerente é a folha — depende de Departamento + Prioridade + Status.

FILTROS = [
    {
        "id":   "e5f6a7b8", "slug": "filtro_prioridade",
        "name": "Prioridade",          "col": "prioridade",
        "filteringParameters": [],
    },
    {
        "id":   "a1b2c3d4", "slug": "filtro_status",
        "name": "Status",              "col": "status",
        "filteringParameters": ["e5f6a7b8"],
    },
    {
        "id":   "e7f8a9b0", "slug": "filtro_departamento",
        "name": "Departamento",        "col": "departamento_relator",
        "filteringParameters": ["e5f6a7b8", "a1b2c3d4"],
    },
    {
        "id":   "a3b4c5d6", "slug": "filtro_gerente",
        "name": "Gerente Responsável", "col": "gerente_responsavel",
        "filteringParameters": ["e7f8a9b0", "e5f6a7b8", "a1b2c3d4"],
    },
    {
        "id":   "c9d0e1f2", "slug": "filtro_jornada",
        "name": "Jornada Impactada",   "col": "jornada_impactada",
        "filteringParameters": ["e5f6a7b8", "a1b2c3d4"],
    },
    {
        "id":   "c1d2e3f4", "slug": "filtro_paliativo",
        "name": "Paliativo",           "col": "paliativo",
        "filteringParameters": ["e5f6a7b8", "a1b2c3d4"],
    },
]


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
        "name":   "FlowETL - SQL Server",
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


# ── 3. Field IDs para os filtros em cascata ───────────────────────────────────
# Field filters precisam do ID real do campo no banco para que o Metabase
# consiga calcular os valores disponíveis e aplicar a cascata nos dropdowns.
step("Obtendo field IDs de tb_problemas_gov_ti")

r = get(f"/api/database/{db_id}/metadata", HDR)
all_tables = r.json().get("tables", [])
table_row = next(
    (t for t in all_tables if t["name"].lower() == "tb_problemas_gov_ti"),
    None,
)
if not table_row:
    print("Tabela não encontrada no metadata. Tentando sync manual...")
    post(f"/api/database/{db_id}/sync_schema", {}, HDR)
    time.sleep(20)
    r = get(f"/api/database/{db_id}/metadata", HDR)
    all_tables = r.json().get("tables", [])
    table_row = next(
        (t for t in all_tables if t["name"].lower() == "tb_problemas_gov_ti"),
        None,
    )
    if not table_row:
        print("Tabela tb_problemas_gov_ti ainda não encontrada. Abortando.")
        sys.exit(1)

r = get(f"/api/table/{table_row['id']}/query_metadata", HDR)
fields_by_name = {f["name"].lower(): f["id"] for f in r.json().get("fields", [])}

filtros_ativos = []
for f in FILTROS:
    fid = fields_by_name.get(f["col"].lower())
    if fid is None:
        print(f"   [aviso] Campo '{f['col']}' não encontrado — filtro '{f['name']}' pulado")
        continue
    f["field_id"] = fid
    filtros_ativos.append(f)
    print(f"   {f['col']}: field_id={fid}")


# ── 4. Template-tags e SQLs com field filters ─────────────────────────────────
def build_template_tags():
    return {
        f["slug"]: {
            "name":         f["slug"],
            "display-name": f["name"],
            "type":         "dimension",
            "dimension":    ["field", f["field_id"], None],
            "widget-type":  "string/=",
            "required":     False,
            "default":      None,
        }
        for f in filtros_ativos
    }


FILTROS_SQL = "\n".join(
    f"[[AND {{{{{f['slug']}}}}}]]" for f in filtros_ativos
)

CARDS = [
    {
        "name":    "[Exec] Total de Problemas",
        "display": "scalar",
        "viz":     {},
        "query": f"""
SELECT COUNT(*) AS total
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FILTROS_SQL}
""",
    },
    {
        "name":    "[Exec] Problemas Abertos",
        "display": "scalar",
        "viz":     {},
        "query": f"""
SELECT COUNT(*) AS abertos
FROM dbo.tb_problemas_gov_ti
WHERE status NOT IN ('Resolvido', 'Cancelado')
{FILTROS_SQL}
""",
    },
    {
        "name":    "[Exec] Críticos P1 em Aberto",
        "display": "scalar",
        "viz":     {},
        "query": f"""
SELECT COUNT(*) AS p1_abertos
FROM dbo.tb_problemas_gov_ti
WHERE prioridade = 'P1'
  AND status NOT IN ('Resolvido', 'Cancelado')
{FILTROS_SQL}
""",
    },
    {
        "name":    "[Exec] Problemas por Status",
        "display": "bar",
        "viz": {
            "graph.dimensions": ["status"],
            "graph.metrics":    ["qt_problemas"],
        },
        "query": f"""
SELECT status, COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FILTROS_SQL}
GROUP BY status
ORDER BY qt_problemas DESC
""",
    },
    {
        "name":    "[Exec] Problemas por Prioridade",
        "display": "pie",
        "viz": {
            "pie.dimension":   "prioridade",
            "pie.metric":      "qt_problemas",
            "pie.show_legend": True,
        },
        "query": f"""
SELECT prioridade, COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FILTROS_SQL}
GROUP BY prioridade
ORDER BY prioridade
""",
    },
    {
        "name":    "[Exec] Problemas por Jornada Impactada",
        "display": "bar",
        "viz": {
            "graph.dimensions": ["jornada_impactada"],
            "graph.metrics":    ["qt_problemas"],
        },
        "query": f"""
SELECT ISNULL(jornada_impactada, 'Não informado') AS jornada_impactada,
       COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FILTROS_SQL}
GROUP BY jornada_impactada
ORDER BY qt_problemas DESC
""",
    },
    {
        "name":    "[Exec] Paliativo — Sim vs Não",
        "display": "pie",
        "viz": {
            "pie.dimension":   "paliativo",
            "pie.metric":      "qt_problemas",
            "pie.show_legend": True,
        },
        "query": f"""
SELECT ISNULL(paliativo, 'Não informado') AS paliativo,
       COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FILTROS_SQL}
GROUP BY paliativo
""",
    },
    {
        "name":    "[Exec] Problemas por Gerente Responsável",
        "display": "bar",
        "viz": {
            "graph.dimensions": ["gerente_responsavel"],
            "graph.metrics":    ["qt_problemas"],
        },
        "query": f"""
SELECT ISNULL(gerente_responsavel, 'Não informado') AS gerente_responsavel,
       COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FILTROS_SQL}
GROUP BY gerente_responsavel
ORDER BY qt_problemas DESC
""",
    },
    {
        "name":    "[Exec] Problemas por Departamento Relator",
        "display": "bar",
        "viz": {
            "graph.dimensions": ["departamento_relator"],
            "graph.metrics":    ["qt_problemas"],
        },
        "query": f"""
SELECT ISNULL(departamento_relator, 'Não informado') AS departamento_relator,
       COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FILTROS_SQL}
GROUP BY departamento_relator
ORDER BY qt_problemas DESC
""",
    },
    {
        "name":    "[Exec] Tabela: Problemas Abertos",
        "display": "table",
        "viz":     {"table.pivot": False},
        "query": f"""
SELECT
  numero,
  prioridade,
  titulo,
  status,
  CONVERT(VARCHAR, dt_abertura, 103) AS dt_abertura,
  ISNULL(jornada_impactada, '—')    AS jornada_impactada,
  ISNULL(gerente_responsavel, '—')  AS gerente_responsavel,
  ISNULL(departamento_relator, '—') AS departamento_relator,
  ISNULL(paliativo, '—')            AS paliativo
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FILTROS_SQL}
ORDER BY
  CASE prioridade WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END,
  dt_abertura
""",
    },
]


# ── 5. Criar ou atualizar cards ───────────────────────────────────────────────
step("Criando/atualizando cards KPI")

r = get("/api/card", HDR)
existentes_por_nome = {
    c["name"]: c["id"]
    for c in (r.json() if isinstance(r.json(), list) else r.json().get("data", []))
}

tt = build_template_tags()
card_ids = []

for card in CARDS:
    payload = {
        "name": card["name"],
        "dataset_query": {
            "type":   "native",
            "native": {"query": card["query"].strip(), "template-tags": tt},
            "database": db_id,
        },
        "display":                  card["display"],
        "visualization_settings":   card["viz"],
        "collection_id":            None,
    }

    if card["name"] in existentes_por_nome:
        cid  = existentes_por_nome[card["name"]]
        resp = put(f"/api/card/{cid}", payload, HDR)
        verb = "atualizado"
    else:
        resp = post("/api/card", payload, HDR)
        verb = "criado"

    if resp.status_code not in (200, 201, 202):
        print(f"   [erro]      {card['name']}: {resp.text[:200]}")
        card_ids.append(None)
        continue

    cid = resp.json()["id"]
    card_ids.append(cid)
    print(f"   [{verb}]  {card['name']} (ID {cid})")


# ── 6. Dashboard ──────────────────────────────────────────────────────────────
step(f"Criando/atualizando dashboard: {DASH_NAME}")

r = get("/api/dashboard", HDR)
dashboards = r.json() if isinstance(r.json(), list) else r.json().get("data", [])
dash_row = next((d for d in dashboards if d.get("name") == DASH_NAME), None)

if dash_row:
    dash_id = dash_row["id"]
    print(f"   Dashboard já existe (ID {dash_id}) — atualizando...")
else:
    r = post("/api/dashboard", {"name": DASH_NAME, "collection_id": None}, HDR)
    if r.status_code not in (200, 201):
        print(f"Erro ao criar dashboard: {r.text}")
        sys.exit(1)
    dash_id = r.json()["id"]
    print(f"   Dashboard criado (ID {dash_id})")

# Parâmetros com hierarquia de cascata
params_dashboard = [
    {
        "id":                  f["id"],
        "name":                f["name"],
        "slug":                f["slug"],
        "type":                "string/=",
        "sectionId":           "string",
        "filteringParameters": f["filteringParameters"],
    }
    for f in filtros_ativos
]

# Layout (grade 24 colunas)
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

# parameter_mappings: para field filters o target é ["dimension", ["template-tag", slug]]
def build_param_mappings(card_id):
    return [
        {
            "parameter_id": f["id"],
            "card_id":      card_id,
            "target":       ["dimension", ["template-tag", f["slug"]]],
        }
        for f in filtros_ativos
    ]

dashcards = [
    {
        "id":                     -(i + 1),
        "card_id":                cid,
        "row":                    pos["row"],
        "col":                    pos["col"],
        "size_x":                 pos["size_x"],
        "size_y":                 pos["size_y"],
        "parameter_mappings":     build_param_mappings(cid),
        "visualization_settings": {},
        "series":                 [],
    }
    for i, (cid, pos) in enumerate(zip(card_ids, LAYOUT))
    if cid is not None
]

r = put(
    f"/api/dashboard/{dash_id}",
    {"parameters": params_dashboard, "dashcards": dashcards},
    HDR,
)
if r.status_code not in (200, 201, 202):
    print(f"   [erro] ao montar dashboard: {r.text[:300]}")
    sys.exit(1)

print(f"   {len(dashcards)} cards posicionados no dashboard.")


# ── Resumo ────────────────────────────────────────────────────────────────────
print(f"""
{'='*55}
Dashboard criado com sucesso!

  URL: https://localhost/dashboard/{dash_id}

  Layout:
    Linha 1 — Scorecards: Total | Abertos | P1 Críticos
    Linha 2 — Status (barra) | Prioridade (pizza)
    Linha 3 — Jornada (barra) | Paliativo (pizza)
    Linha 4 — Gerente (barra) | Departamento (barra)
    Linha 5 — Tabela completa

  Filtros em cascata:
    Prioridade → Status → Departamento → Gerente
                       ↘ Jornada Impactada
                       ↘ Paliativo
{'='*55}
""")
