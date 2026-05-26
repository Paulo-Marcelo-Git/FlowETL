"""
Cria o dashboard "Dashboard Executivo · Gestão de Problemas" no Metabase.

Cards (15):
  Q1–Q8   KPI numéricos (Total + 1 por status)
  Q9      Donut — Distribuição por Status
  Q10     Donut — Paliativo Disponível
  Q11     Barra horizontal — Jornada Impactada
  Q12     Barra empilhada — Volume por Gerente x Status
  Q13     Tabela — Status por Gerente
  Q14     Tabela — Registros Concluídos
  Q15     Tabela — Resumo de Atualizações · Todos os Registros

Filtros em cascata (5):
  Gerente Responsável → Status → Prioridade → Jornada → Paliativo

Uso:
    python scripts/dashboard_govti.py
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
DASH_NAME   = "Dashboard Executivo · Gestão de Problemas"
COLLECTION  = "GOV TI"

if not ADMIN_EMAIL or not ADMIN_SENHA:
    print("❌ Erro: MB_ADMIN_USER e MB_ADMIN_PASS devem estar definidas no .env")
    sys.exit(1)


# ── helpers ───────────────────────────────────────────────────────────────────

def _req(method, path, payload=None, headers=None):
    fn = getattr(requests, method)
    kwargs = {"verify": False, "timeout": 30}
    if headers:
        kwargs["headers"] = headers
    if payload is not None:
        kwargs["json"] = payload
    return fn(f"{BASE}{path}", **kwargs)

def get(path, headers=None):
    return _req("get", path, headers=headers)

def post(path, payload, headers=None):
    return _req("post", path, payload, headers)

def put(path, payload, headers=None):
    return _req("put", path, payload, headers)

def step(msg):
    print(f"\n{'─'*60}\n▶  {msg}")

def ok(msg):
    print(f"   ✔  {msg}")

def warn(msg):
    print(f"   ⚠  {msg}")

def err(msg):
    print(f"   ✘  {msg}")


# ── Definição dos filtros em cascata ──────────────────────────────────────────
# filteringParameters: IDs dos filtros cujas seleções restringem este.
# Cascata sugerida: Gerente → Status → Prioridade → Jornada → Paliativo

FILTROS = [
    {
        "id":   "f1a2b3c4", "slug": "filtro_gerente",
        "name": "Gerente Responsável", "col": "gerente_responsavel",
        "filteringParameters": [],
    },
    {
        "id":   "d5e6f7a8", "slug": "filtro_status",
        "name": "Status",              "col": "status",
        "filteringParameters": ["f1a2b3c4"],
    },
    {
        "id":   "b9c0d1e2", "slug": "filtro_prioridade",
        "name": "Prioridade",          "col": "prioridade",
        "filteringParameters": ["f1a2b3c4", "d5e6f7a8"],
    },
    {
        "id":   "f3a4b5c6", "slug": "filtro_jornada",
        "name": "Jornada Impactada",   "col": "jornada_impactada",
        "filteringParameters": ["f1a2b3c4", "d5e6f7a8", "b9c0d1e2"],
    },
    {
        "id":   "d7e8f9a0", "slug": "filtro_paliativo",
        "name": "Paliativo",           "col": "paliativo",
        "filteringParameters": ["f1a2b3c4", "d5e6f7a8", "b9c0d1e2", "f3a4b5c6"],
    },
]

# IDs dos KPIs de status fixo (Q2–Q8) — o filtro "Status" NÃO é mapeado
# nesses cards, pois cada um já filtra um status fixo em SQL.
# Os outros 4 filtros (Gerente, Prioridade, Jornada, Paliativo) SÃO mapeados.
FILTROS_SEM_STATUS = {"filtro_status"}


# ── 1. Login ──────────────────────────────────────────────────────────────────
step("Login no Metabase")
r = post("/api/session", {"username": ADMIN_EMAIL, "password": ADMIN_SENHA})
if r.status_code != 200:
    err(f"Falha no login: {r.text}")
    sys.exit(1)
session_id = r.json()["id"]
HDR = {"X-Metabase-Session": session_id, "Content-Type": "application/json"}
ok(f"Sessão iniciada ({ADMIN_EMAIL})")


# ── 2. Banco de dados ─────────────────────────────────────────────────────────
step("Obtendo ID do banco SQL Server")
r = get("/api/database", HDR)
dbs = r.json().get("data", r.json() if isinstance(r.json(), list) else [])
db_row = next((d for d in dbs if d.get("name") == "FlowETL - SQL Server"), None)

if db_row:
    db_id = db_row["id"]
    ok(f"Banco encontrado (ID {db_id})")
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
        err(f"Erro ao conectar banco: {r.text}")
        sys.exit(1)
    db_id = r.json()["id"]
    ok(f"SQL Server registrado (ID {db_id}). Aguardando sync (20s)...")
    time.sleep(20)


# ── 3. Coleção "GOV TI" ───────────────────────────────────────────────────────
step(f"Obtendo/criando coleção '{COLLECTION}'")
r = get("/api/collection", HDR)
raw = r.json()
colecoes = raw if isinstance(raw, list) else raw.get("data", [])
col_row = next((c for c in colecoes if isinstance(c, dict) and c.get("name") == COLLECTION), None)

if col_row:
    col_id = col_row["id"]
    ok(f"Coleção '{COLLECTION}' encontrada (ID {col_id})")
else:
    r = post("/api/collection", {"name": COLLECTION, "color": "#509EE3"}, HDR)
    if r.status_code not in (200, 201):
        warn(f"Não foi possível criar coleção '{COLLECTION}': {r.text[:100]}. Usando raiz.")
        col_id = None
    else:
        col_id = r.json()["id"]
        ok(f"Coleção '{COLLECTION}' criada (ID {col_id})")


# ── 4. Field IDs para field filters ──────────────────────────────────────────
step("Obtendo field IDs de tb_problemas_gov_ti")

r = get(f"/api/database/{db_id}/metadata", HDR)
all_tables = r.json().get("tables", [])
table_row = next(
    (t for t in all_tables if t["name"].lower() == "tb_problemas_gov_ti"),
    None,
)
if not table_row:
    warn("Tabela não encontrada no metadata — forçando sync...")
    post(f"/api/database/{db_id}/sync_schema", {}, HDR)
    time.sleep(25)
    r = get(f"/api/database/{db_id}/metadata", HDR)
    all_tables = r.json().get("tables", [])
    table_row = next(
        (t for t in all_tables if t["name"].lower() == "tb_problemas_gov_ti"),
        None,
    )
    if not table_row:
        err("Tabela tb_problemas_gov_ti não encontrada. Abortando.")
        sys.exit(1)

r = get(f"/api/table/{table_row['id']}/query_metadata", HDR)
fields_by_name = {f["name"].lower(): f["id"] for f in r.json().get("fields", [])}

filtros_ativos = []
for f in FILTROS:
    fid = fields_by_name.get(f["col"].lower())
    if fid is None:
        warn(f"Campo '{f['col']}' não encontrado — filtro '{f['name']}' pulado")
        continue
    f["field_id"] = fid
    filtros_ativos.append(f)
    ok(f"{f['col']} → field_id={fid}")

filtros_por_slug = {f["slug"]: f for f in filtros_ativos}


# ── 5. Construção das template-tags e SQL fragments ───────────────────────────

def build_template_tags(excluir_slugs=None):
    """Retorna o dict de template-tags para o payload de card nativo."""
    excluir = set(excluir_slugs or [])
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
        if f["slug"] not in excluir
    }


def filtros_sql(excluir_slugs=None):
    """Retorna as linhas [[AND ...]] para embedding no SQL."""
    excluir = set(excluir_slugs or [])
    return "\n".join(
        f"  [[AND {{{{  {f['slug']}  }}}}]]"
        for f in filtros_ativos
        if f["slug"] not in excluir
    )


# SQL fragment padrão (todos os 5 filtros)
FS_ALL = filtros_sql()
# Sem o filtro de status (usado nos KPIs de status fixo)
FS_NO_STATUS = filtros_sql(excluir_slugs=["filtro_status"])


# ── 6. Definição dos 15 cards ─────────────────────────────────────────────────

# Cada card: name, display, viz, query, excluir_slugs (None = todos os filtros)
CARDS = [

    # ── BLOCO 1: KPIs numéricos ───────────────────────────────────────────────

    {   # Q1
        "name": "[GovTI] Total de Registros",
        "display": "scalar",
        "viz": {},
        "query": f"""
SELECT COUNT(*) AS total
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FS_ALL}
""",
        "excluir_slugs": [],
    },
    {   # Q2
        "name": "[GovTI] Investigação",
        "display": "scalar",
        "viz": {},
        "query": f"""
SELECT COUNT(*) AS qt
FROM dbo.tb_problemas_gov_ti
WHERE status = 'Investigação'
{FS_NO_STATUS}
""",
        "excluir_slugs": ["filtro_status"],
    },
    {   # Q3
        "name": "[GovTI] Resolvido",
        "display": "scalar",
        "viz": {},
        "query": f"""
SELECT COUNT(*) AS qt
FROM dbo.tb_problemas_gov_ti
WHERE status = 'Resolvido'
{FS_NO_STATUS}
""",
        "excluir_slugs": ["filtro_status"],
    },
    {   # Q4
        "name": "[GovTI] Validação",
        "display": "scalar",
        "viz": {},
        "query": f"""
SELECT COUNT(*) AS qt
FROM dbo.tb_problemas_gov_ti
WHERE status = 'Validação'
{FS_NO_STATUS}
""",
        "excluir_slugs": ["filtro_status"],
    },
    {   # Q5
        "name": "[GovTI] Em Implementação",
        "display": "scalar",
        "viz": {},
        "query": f"""
SELECT COUNT(*) AS qt
FROM dbo.tb_problemas_gov_ti
WHERE status = 'Em Implementação'
{FS_NO_STATUS}
""",
        "excluir_slugs": ["filtro_status"],
    },
    {   # Q6
        "name": "[GovTI] Plan. Solução",
        "display": "scalar",
        "viz": {},
        "query": f"""
SELECT COUNT(*) AS qt
FROM dbo.tb_problemas_gov_ti
WHERE status = 'Plan. Solução'
{FS_NO_STATUS}
""",
        "excluir_slugs": ["filtro_status"],
    },
    {   # Q7
        "name": "[GovTI] Novo",
        "display": "scalar",
        "viz": {},
        "query": f"""
SELECT COUNT(*) AS qt
FROM dbo.tb_problemas_gov_ti
WHERE status = 'Novo'
{FS_NO_STATUS}
""",
        "excluir_slugs": ["filtro_status"],
    },
    {   # Q8
        "name": "[GovTI] Cancelado",
        "display": "scalar",
        "viz": {},
        "query": f"""
SELECT COUNT(*) AS qt
FROM dbo.tb_problemas_gov_ti
WHERE status = 'Cancelado'
{FS_NO_STATUS}
""",
        "excluir_slugs": ["filtro_status"],
    },

    # ── BLOCO 2: Gráficos ─────────────────────────────────────────────────────

    {   # Q9
        "name": "[GovTI] Distribuição por Status",
        "display": "pie",
        "viz": {
            "pie.dimension":   "status",
            "pie.metric":      "qt_problemas",
            "pie.show_legend": True,
            "column_settings": {
                '["name","status"]': {
                    "color_mapping": {
                        "Investigação":      "#4f8ef7",
                        "Resolvido":         "#30c97e",
                        "Validação":         "#ab87ff",
                        "Em Implementação":  "#22d3c8",
                        "Plan. Solução":     "#ff7c3e",
                        "Novo":              "#f5b942",
                        "Cancelado":         "#6b7280",
                    },
                },
            },
        },
        "query": f"""
SELECT status, COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FS_ALL}
GROUP BY status
ORDER BY qt_problemas DESC
""",
        "excluir_slugs": [],
    },
    {   # Q10
        "name": "[GovTI] Paliativo Disponível",
        "display": "pie",
        "viz": {
            "pie.dimension":   "paliativo",
            "pie.metric":      "qt_problemas",
            "pie.show_legend": True,
            "column_settings": {
                '["name","paliativo"]': {
                    "color_mapping": {
                        "Sim":           "#30c97e",
                        "Não":           "#ef4444",
                        "Não informado": "#6b7280",
                    },
                },
            },
        },
        "query": f"""
SELECT
  ISNULL(paliativo, 'Não informado') AS paliativo,
  COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FS_ALL}
GROUP BY paliativo
""",
        "excluir_slugs": [],
    },
    {   # Q11
        "name": "[GovTI] Jornada Impactada",
        "display": "bar",
        "viz": {
            "graph.dimensions":    ["jornada_impactada"],
            "graph.metrics":       ["qt_problemas"],
            "graph.x_axis.axis_enabled": True,
            "graph.y_axis.axis_enabled": True,
            "stackable.stack_type": None,
            "graph.label_value_formatting": "auto",
        },
        "query": f"""
SELECT
  ISNULL(jornada_impactada, 'Não informado') AS jornada_impactada,
  COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FS_ALL}
GROUP BY jornada_impactada
ORDER BY qt_problemas DESC
""",
        "excluir_slugs": [],
    },
    {   # Q12
        "name": "[GovTI] Volume por Gerente Responsável",
        "display": "bar",
        "viz": {
            "graph.dimensions":     ["gerente_responsavel", "status"],
            "graph.metrics":        ["qt_problemas"],
            "stackable.stack_type": "stacked",
            "graph.x_axis.title_text": "Gerente",
            "graph.y_axis.title_text": "Quantidade",
        },
        "query": f"""
SELECT
  ISNULL(gerente_responsavel, 'Não informado') AS gerente_responsavel,
  status,
  COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FS_ALL}
GROUP BY gerente_responsavel, status
ORDER BY gerente_responsavel, qt_problemas DESC
""",
        "excluir_slugs": [],
    },
    {   # Q13
        "name": "[GovTI] Status por Gerente",
        "display": "table",
        "viz": {
            "table.pivot": False,
            "column_settings": {},
        },
        "query": f"""
SELECT
  ISNULL(gerente_responsavel, 'Não informado') AS Gerente,
  status                                        AS Status,
  COUNT(*)                                      AS Qtd,
  SUM(COUNT(*)) OVER (PARTITION BY gerente_responsavel) AS Total
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FS_ALL}
GROUP BY gerente_responsavel, status
ORDER BY gerente_responsavel, Qtd DESC
""",
        "excluir_slugs": [],
    },

    # ── BLOCO 3: Tabela de Resolvidos ─────────────────────────────────────────

    {   # Q14
        "name": "[GovTI] Registros Concluídos · Datas e Responsáveis",
        "display": "table",
        "viz": {
            "table.pivot": False,
            "table.columns": [
                {"name": "Nº",                "enabled": True},
                {"name": "Título do Problema","enabled": True},
                {"name": "Gerente",           "enabled": True},
                {"name": "Data de Conclusão", "enabled": True},
                {"name": "Status",            "enabled": True},
            ],
        },
        "query": f"""
SELECT
  numero                AS [Nº],
  titulo                AS [Título do Problema],
  gerente_responsavel   AS [Gerente],
  dt_conclusao          AS [Data de Conclusão],
  status                AS [Status]
FROM dbo.tb_problemas_gov_ti
WHERE status = 'Resolvido'
{FS_NO_STATUS}
ORDER BY numero
""",
        "excluir_slugs": ["filtro_status"],
    },

    # ── BLOCO 4: Tabela completa de último status ─────────────────────────────

    {   # Q15
        "name": "[GovTI] Resumo de Atualizações · Todos os Registros",
        "display": "table",
        "viz": {
            "table.pivot": False,
            "column_settings": {
                '["name","Status"]': {
                    "column_title": "Status",
                    "click_behavior": {},
                },
            },
            "table.cell_column": "Status",
            "conditional_formatting": [
                {"color": "#4f8ef7", "operator": "=", "value": "Investigação",     "column": "Status", "highlight_row": False},
                {"color": "#30c97e", "operator": "=", "value": "Resolvido",         "column": "Status", "highlight_row": False},
                {"color": "#ab87ff", "operator": "=", "value": "Validação",         "column": "Status", "highlight_row": False},
                {"color": "#22d3c8", "operator": "=", "value": "Em Implementação",  "column": "Status", "highlight_row": False},
                {"color": "#ff7c3e", "operator": "=", "value": "Plan. Solução",     "column": "Status", "highlight_row": False},
                {"color": "#f5b942", "operator": "=", "value": "Novo",              "column": "Status", "highlight_row": False},
                {"color": "#6b7280", "operator": "=", "value": "Cancelado",         "column": "Status", "highlight_row": False},
                # Paliativo
                {"color": "#30c97e", "operator": "=", "value": "Sim",               "column": "Paliativo", "highlight_row": False},
                {"color": "#ef4444", "operator": "=", "value": "Não",               "column": "Paliativo", "highlight_row": False},
                # Prioridade
                {"color": "#ef4444", "operator": "=", "value": "P1",                "column": "Prior.", "highlight_row": False},
                {"color": "#ff7c3e", "operator": "=", "value": "P2",                "column": "Prior.", "highlight_row": False},
                {"color": "#f5b942", "operator": "=", "value": "P3",                "column": "Prior.", "highlight_row": False},
                {"color": "#6b7280", "operator": "=", "value": "P4",                "column": "Prior.", "highlight_row": False},
            ],
        },
        "query": f"""
SELECT
  numero                AS [Nº],
  prioridade            AS [Prior.],
  titulo                AS [Título],
  gerente_responsavel   AS [Gerente],
  status                AS [Status],
  paliativo             AS [Paliativo],
  status_27_04          AS [Último Status]
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FS_ALL}
ORDER BY
  CASE status
    WHEN 'Investigação'     THEN 1
    WHEN 'Novo'             THEN 2
    WHEN 'Plan. Solução'    THEN 3
    WHEN 'Em Implementação' THEN 4
    WHEN 'Validação'        THEN 5
    WHEN 'Resolvido'        THEN 6
    WHEN 'Cancelado'        THEN 7
    ELSE 8
  END,
  prioridade ASC,
  numero ASC
""",
        "excluir_slugs": [],
    },
]


# ── 7. Criar ou atualizar cards ───────────────────────────────────────────────
step("Criando/atualizando os 15 cards")

r = get("/api/card", HDR)
existentes_por_nome = {
    c["name"]: c["id"]
    for c in (r.json() if isinstance(r.json(), list) else r.json().get("data", []))
}

card_ids = []

for i, card in enumerate(CARDS, start=1):
    excluir = card.get("excluir_slugs") or []
    tt = build_template_tags(excluir_slugs=excluir)

    payload = {
        "name": card["name"],
        "dataset_query": {
            "type":   "native",
            "native": {"query": card["query"].strip(), "template-tags": tt},
            "database": db_id,
        },
        "display":                card["display"],
        "visualization_settings": card["viz"],
        "collection_id":          col_id,
    }

    if card["name"] in existentes_por_nome:
        cid  = existentes_por_nome[card["name"]]
        resp = put(f"/api/card/{cid}", payload, HDR)
        verb = "atualizado"
    else:
        resp = post("/api/card", payload, HDR)
        verb = "criado"

    if resp.status_code not in (200, 201, 202):
        err(f"Q{i} {card['name']}: {resp.text[:200]}")
        card_ids.append(None)
        continue

    cid = resp.json()["id"]
    card_ids.append(cid)
    ok(f"Q{i} [{verb}] {card['name']} (ID {cid})")


# ── 8. Dashboard ──────────────────────────────────────────────────────────────
step(f"Criando/atualizando dashboard: '{DASH_NAME}'")

r = get("/api/dashboard", HDR)
dashboards = r.json() if isinstance(r.json(), list) else r.json().get("data", [])
dash_row = next((d for d in dashboards if d.get("name") == DASH_NAME), None)

if dash_row:
    dash_id = dash_row["id"]
    ok(f"Dashboard já existe (ID {dash_id}) — atualizando...")
else:
    r = post("/api/dashboard", {"name": DASH_NAME, "collection_id": col_id}, HDR)
    if r.status_code not in (200, 201):
        err(f"Erro ao criar dashboard: {r.text}")
        sys.exit(1)
    dash_id = r.json()["id"]
    ok(f"Dashboard criado (ID {dash_id})")


# ── 9. Parâmetros do dashboard (com cascata) ──────────────────────────────────

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


# ── 10. Layout: grade de 24 colunas ──────────────────────────────────────────
#
#  Linha 0  (h=4):  Q1 (col0,w6) | Q2 (col6,w3) | Q3 (col9,w3) | Q4 (col12,w3)
#                   Q5 (col15,w3)| Q6 (col18,w3) | Q7 (col21,w1+w2 split)
#
#  Metabase usa grade de 24 cols. Distribuição dos 8 KPIs:
#    Q1 Total → maior (w=6), Q2-Q8 → w=3 cada (7 * 3 = 21; total 6+18=24? não.
#  Melhor: Q1 w=6, Q2-Q8 w=3 (7 cards x 3 = 21 → total 27 > 24).
#  Ajuste: Q1 w=6, Q2-Q5 w=4 (4x4=16), Q6-Q8 já não cabem na mesma linha.
#  Solução: 2 linhas de KPIs.
#    Linha 0: Q1(w6) Q2(w6) Q3(w6) Q4(w6)               → total 24
#    Linha 4: Q5(w6) Q6(w6) Q7(w6) Q8(w6)               → total 24
#
#  Linha 8  (h=8):  Q9 Donut Status (w8) | Q10 Donut Paliativo (w8) | Q11 Jornada (w8)
#  Linha 16 (h=9):  Q12 Barras Empilhadas (w12) | Q13 Tabela Status/Gerente (w12)
#  Linha 25 (h=8):  Q14 Tabela Resolvidos (w24)
#  Linha 33 (h=12): Q15 Tabela Resumo Total (w24)

LAYOUT = [
    # Linha 0 — KPIs superiores (Q1–Q4)
    {"row": 0,  "col": 0,  "size_x": 6,  "size_y": 4},   # Q1  Total
    {"row": 0,  "col": 6,  "size_x": 6,  "size_y": 4},   # Q2  Investigação
    {"row": 0,  "col": 12, "size_x": 6,  "size_y": 4},   # Q3  Resolvido
    {"row": 0,  "col": 18, "size_x": 6,  "size_y": 4},   # Q4  Validação

    # Linha 4 — KPIs inferiores (Q5–Q8)
    {"row": 4,  "col": 0,  "size_x": 6,  "size_y": 4},   # Q5  Em Implementação
    {"row": 4,  "col": 6,  "size_x": 6,  "size_y": 4},   # Q6  Plan. Solução
    {"row": 4,  "col": 12, "size_x": 6,  "size_y": 4},   # Q7  Novo
    {"row": 4,  "col": 18, "size_x": 6,  "size_y": 4},   # Q8  Cancelado

    # Linha 8 — Gráficos: 3 colunas (Q9, Q10, Q11)
    {"row": 8,  "col": 0,  "size_x": 8,  "size_y": 9},   # Q9  Donut Status
    {"row": 8,  "col": 8,  "size_x": 8,  "size_y": 9},   # Q10 Donut Paliativo
    {"row": 8,  "col": 16, "size_x": 8,  "size_y": 9},   # Q11 Barras Jornada

    # Linha 17 — Gerente empilhado + tabela por gerente (Q12, Q13)
    {"row": 17, "col": 0,  "size_x": 14, "size_y": 10},  # Q12 Barras Empilhadas
    {"row": 17, "col": 14, "size_x": 10, "size_y": 10},  # Q13 Tabela Gerente

    # Linha 27 — Tabela de Resolvidos (Q14)
    {"row": 27, "col": 0,  "size_x": 24, "size_y": 8},   # Q14 Resolvidos

    # Linha 35 — Tabela Resumo Completo (Q15) — alta para scroll
    {"row": 35, "col": 0,  "size_x": 24, "size_y": 14},  # Q15 Todos os Registros
]


# ── 11. Mapeamento filtro → card ──────────────────────────────────────────────
#
# Regra:
#  - Q1 (Total)  → todos os 5 filtros
#  - Q2-Q8 (KPI status fixo) → apenas Gerente, Prioridade, Jornada, Paliativo
#  - Q9-Q13, Q15 → todos os 5 filtros
#  - Q14 (Resolvidos, status fixo) → apenas Gerente, Prioridade, Jornada, Paliativo

# Índices base-0 dos cards cujo filtro "status" é excluído
CARDS_SEM_STATUS = {1, 2, 3, 4, 5, 6, 7, 13}  # Q2–Q8 e Q14


def build_param_mappings(card_idx, card_id):
    """Retorna lista de parameter_mappings para um card."""
    mappings = []
    for f in filtros_ativos:
        if f["slug"] == "filtro_status" and card_idx in CARDS_SEM_STATUS:
            continue
        mappings.append({
            "parameter_id": f["id"],
            "card_id":      card_id,
            "target":       ["dimension", ["template-tag", f["slug"]]],
        })
    return mappings


dashcards = []
for i, (cid, pos) in enumerate(zip(card_ids, LAYOUT)):
    if cid is None:
        warn(f"Card Q{i+1} sem ID — pulado no dashboard")
        continue
    dashcards.append({
        "id":                     -(i + 1),
        "card_id":                cid,
        "row":                    pos["row"],
        "col":                    pos["col"],
        "size_x":                 pos["size_x"],
        "size_y":                 pos["size_y"],
        "parameter_mappings":     build_param_mappings(i, cid),
        "visualization_settings": {},
        "series":                 [],
    })


r = put(
    f"/api/dashboard/{dash_id}",
    {"parameters": params_dashboard, "dashcards": dashcards},
    HDR,
)
if r.status_code not in (200, 201, 202):
    err(f"Erro ao montar dashboard: {r.text[:400]}")
    sys.exit(1)

ok(f"{len(dashcards)} cards posicionados.")


# ── 12. Resumo ────────────────────────────────────────────────────────────────
criados  = sum(1 for c in card_ids if c is not None)
pulados  = sum(1 for c in card_ids if c is None)

print(f"""
{'='*60}
Dashboard configurado com sucesso!

  URL:      {BASE}/dashboard/{dash_id}
  Nome:     {DASH_NAME}
  Coleção:  {COLLECTION} (ID {col_id})
  Cards:    {criados} criados/atualizados  |  {pulados} com erro

  Layout:
    Linha 1 — KPIs: Total | Investigação | Resolvido | Validação
    Linha 2 — KPIs: Em Implementação | Plan.Solução | Novo | Cancelado
    Linha 3 — Donut Status | Donut Paliativo | Barras Jornada
    Linha 4 — Barras Empilhadas Gerente | Tabela Status por Gerente
    Linha 5 — Tabela Resolvidos (largura total)
    Linha 6 — Tabela Resumo · Todos os Registros (largura total)

  Filtros em cascata:
    Gerente → Status → Prioridade → Jornada → Paliativo
{'='*60}
""")
