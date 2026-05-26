"""
Implementa filtros em cascata no Dashboard Executivo do Metabase.

Diferenças em relação a configurar_filtros_dashboard.py:
  - Template tags: type "text"  →  type "dimension" (field filter com referência
    ao campo real da tabela — permite que o Metabase calcule valores disponíveis)
  - SQL: [[AND col = {{var}}]]  →  [[AND {{var}}]]
    (o field filter gera "col = valor" automaticamente)
  - Dashboard parameters ganham "filteringParameters" — cascata real

Hierarquia de cascata:
  Prioridade ──→ Status
             ──→ Departamento ──→ Gerente Responsável
             ──→ Jornada Impactada
             ──→ Paliativo
  Status     ──→ Departamento
             ──→ Gerente Responsável
             ──→ Jornada Impactada
             ──→ Paliativo

Pré-requisito: dashboard_executivo.py já foi executado e criou os cards [Exec].
"""

import os
import sys
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
DB_NAME_MB  = os.getenv("MB_DB_DISPLAY_NAME", "FlowETL - SQL Server")
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


# ── Definição dos filtros com hierarquia de cascata ───────────────────────────
# filteringParameters: lista de IDs cujo valor atual restringe as opções deste filtro.
# Ex: Gerente tem [dept_id, prio_id, status_id] → ao selecionar Departamento="TI",
# o dropdown de Gerente mostra só gerentes daquele departamento.

FILTROS = [
    {
        "id":                  "e5f6a7b8",
        "slug":                "filtro_prioridade",
        "name":                "Prioridade",
        "col":                 "prioridade",
        "filteringParameters": [],                              # topo da hierarquia
    },
    {
        "id":                  "a1b2c3d4",
        "slug":                "filtro_status",
        "name":                "Status",
        "col":                 "status",
        "filteringParameters": ["e5f6a7b8"],                   # filtrado por Prioridade
    },
    {
        "id":                  "e7f8a9b0",
        "slug":                "filtro_departamento",
        "name":                "Departamento",
        "col":                 "departamento_relator",
        "filteringParameters": ["e5f6a7b8", "a1b2c3d4"],       # filtrado por Prio + Status
    },
    {
        "id":                  "a3b4c5d6",
        "slug":                "filtro_gerente",
        "name":                "Gerente Responsável",
        "col":                 "gerente_responsavel",
        "filteringParameters": ["e7f8a9b0", "e5f6a7b8", "a1b2c3d4"],  # Dept + Prio + Status
    },
    {
        "id":                  "c9d0e1f2",
        "slug":                "filtro_jornada",
        "name":                "Jornada Impactada",
        "col":                 "jornada_impactada",
        "filteringParameters": ["e5f6a7b8", "a1b2c3d4"],       # filtrado por Prio + Status
    },
    {
        "id":                  "c1d2e3f4",
        "slug":                "filtro_paliativo",
        "name":                "Paliativo",
        "col":                 "paliativo",
        "filteringParameters": ["e5f6a7b8", "a1b2c3d4"],       # filtrado por Prio + Status
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
step(f"Obtendo ID do banco: {DB_NAME_MB}")
r = get("/api/database", HDR)
dbs = r.json().get("data", r.json() if isinstance(r.json(), list) else [])
db_row = next((d for d in dbs if d.get("name") == DB_NAME_MB), None)
if not db_row:
    print(f"Banco '{DB_NAME_MB}' não encontrado no Metabase.")
    sys.exit(1)
db_id = db_row["id"]
print(f"   Banco ID: {db_id}")


# ── 3. Field IDs de tb_problemas_gov_ti ───────────────────────────────────────
# Field filters exigem o ID real do campo no banco para que o Metabase possa
# calcular os valores disponíveis no dropdown (e filtrar em cascata).
step("Obtendo field IDs de tb_problemas_gov_ti")

r = get(f"/api/database/{db_id}/metadata", HDR)
all_tables = r.json().get("tables", [])
table_row = next(
    (t for t in all_tables if t["name"].lower() == "tb_problemas_gov_ti"),
    None,
)
if not table_row:
    print("Tabela tb_problemas_gov_ti não encontrada. Verifique se o sync foi executado.")
    sys.exit(1)

r = get(f"/api/table/{table_row['id']}/query_metadata", HDR)
fields_by_name = {f["name"].lower(): f["id"] for f in r.json().get("fields", [])}
print(f"   Campos encontrados: {sorted(fields_by_name.keys())}")

filtros_com_fid = []
for f in FILTROS:
    fid = fields_by_name.get(f["col"].lower())
    if fid is None:
        print(f"   [AVISO] Campo '{f['col']}' não encontrado — filtro '{f['name']}' ignorado")
        continue
    f["field_id"] = fid
    filtros_com_fid.append(f)
    print(f"   {f['col']}: field_id={fid}")

if not filtros_com_fid:
    print("Nenhum campo encontrado. Abortando.")
    sys.exit(1)


# ── 4. Montar template-tags e SQLs ────────────────────────────────────────────
def build_template_tags():
    """Template tags como field filters — o Metabase resolve col=valor automaticamente."""
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
        for f in filtros_com_fid
    }


# Com field filters a cláusula é [[AND {{slug}}]] — sem "col = " explícito
FILTROS_SQL = "\n".join(f"[[AND {{{{{f['slug']}}}}}]]" for f in filtros_com_fid)

CARDS_SQL = {
    "[Exec] Total de Problemas": f"""
SELECT COUNT(*) AS total
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FILTROS_SQL}
""",
    "[Exec] Problemas Abertos": f"""
SELECT COUNT(*) AS abertos
FROM dbo.tb_problemas_gov_ti
WHERE status NOT IN ('Resolvido', 'Cancelado')
{FILTROS_SQL}
""",
    "[Exec] Críticos P1 em Aberto": f"""
SELECT COUNT(*) AS p1_abertos
FROM dbo.tb_problemas_gov_ti
WHERE prioridade = 'P1'
  AND status NOT IN ('Resolvido', 'Cancelado')
{FILTROS_SQL}
""",
    "[Exec] Problemas por Status": f"""
SELECT status, COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FILTROS_SQL}
GROUP BY status
ORDER BY qt_problemas DESC
""",
    "[Exec] Problemas por Prioridade": f"""
SELECT prioridade, COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FILTROS_SQL}
GROUP BY prioridade
ORDER BY prioridade
""",
    "[Exec] Problemas por Jornada Impactada": f"""
SELECT ISNULL(jornada_impactada, 'Não informado') AS jornada_impactada,
       COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FILTROS_SQL}
GROUP BY jornada_impactada
ORDER BY qt_problemas DESC
""",
    "[Exec] Paliativo — Sim vs Não": f"""
SELECT ISNULL(paliativo, 'Não informado') AS paliativo,
       COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FILTROS_SQL}
GROUP BY paliativo
""",
    "[Exec] Problemas por Gerente Responsável": f"""
SELECT ISNULL(gerente_responsavel, 'Não informado') AS gerente_responsavel,
       COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FILTROS_SQL}
GROUP BY gerente_responsavel
ORDER BY qt_problemas DESC
""",
    "[Exec] Problemas por Departamento Relator": f"""
SELECT ISNULL(departamento_relator, 'Não informado') AS departamento_relator,
       COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FILTROS_SQL}
GROUP BY departamento_relator
ORDER BY qt_problemas DESC
""",
    "[Exec] Tabela: Problemas Abertos": f"""
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
}


# ── 5. Atualizar cards ────────────────────────────────────────────────────────
step("Atualizando cards com field filter template-tags")
r = get("/api/card", HDR)
todos_cards = r.json() if isinstance(r.json(), list) else r.json().get("data", [])
cards_por_nome = {c["name"]: c for c in todos_cards}

tt = build_template_tags()
card_ids_por_nome = {}

for nome, sql in CARDS_SQL.items():
    card = cards_por_nome.get(nome)
    if not card:
        print(f"   [não encontrado] {nome}")
        continue

    payload = {
        "dataset_query": {
            "type":     "native",
            "native":   {"query": sql.strip(), "template-tags": tt},
            "database": db_id,
        },
    }
    resp = put(f"/api/card/{card['id']}", payload, HDR)
    if resp.status_code not in (200, 202):
        print(f"   [erro] {nome}: {resp.text[:200]}")
    else:
        card_ids_por_nome[nome] = card["id"]
        print(f"   [ok] {nome} (ID {card['id']})")


# ── 6. Dashboard: parâmetros com cascata ──────────────────────────────────────
step(f"Buscando dashboard: {DASH_NAME}")
r = get("/api/dashboard", HDR)
dashboards = r.json() if isinstance(r.json(), list) else r.json().get("data", [])
dash_row = next((d for d in dashboards if d.get("name") == DASH_NAME), None)
if not dash_row:
    print(f"Dashboard '{DASH_NAME}' não encontrado.")
    sys.exit(1)

dash_id = dash_row["id"]
r = get(f"/api/dashboard/{dash_id}", HDR)
dash_detail = r.json()
dashcards_atuais = dash_detail.get("dashcards", [])
print(f"   Dashboard ID {dash_id} — {len(dashcards_atuais)} card(s) no layout")

step("Configurando parâmetros com filteringParameters")

params_dashboard = [
    {
        "id":                  f["id"],
        "name":                f["name"],
        "slug":                f["slug"],
        "type":                "string/=",
        "sectionId":           "string",
        "filteringParameters": f["filteringParameters"],
    }
    for f in filtros_com_fid
]

# Para field filters, o target do parameter_mapping é
# ["dimension", ["template-tag", slug]] — não mais ["variable", ...]
dashcards_atualizados = []
for dc in dashcards_atuais:
    card_id   = dc.get("card_id")
    nome_card = next((n for n, cid in card_ids_por_nome.items() if cid == card_id), None)

    param_mappings = []
    if nome_card:
        for f in filtros_com_fid:
            param_mappings.append({
                "parameter_id": f["id"],
                "card_id":      card_id,
                "target":       ["dimension", ["template-tag", f["slug"]]],
            })

    dashcards_atualizados.append({
        "id":                     dc["id"],
        "card_id":                card_id,
        "row":                    dc["row"],
        "col":                    dc["col"],
        "size_x":                 dc["size_x"],
        "size_y":                 dc["size_y"],
        "parameter_mappings":     param_mappings,
        "visualization_settings": dc.get("visualization_settings") or {},
    })

resp = put(
    f"/api/dashboard/{dash_id}",
    {
        "parameters": params_dashboard,
        "dashcards":  dashcards_atualizados,
    },
    HDR,
)
if resp.status_code not in (200, 202):
    print(f"   [erro] ao atualizar dashboard: {resp.text[:400]}")
    sys.exit(1)

print(f"   {len(params_dashboard)} parâmetros configurados")
print(f"   {len(dashcards_atualizados)} cards mapeados")


# ── Resumo ────────────────────────────────────────────────────────────────────
print(f"""
{'='*55}
Filtros em cascata configurados com sucesso!

  Dashboard: https://localhost/dashboard/{dash_id}

  Hierarquia:
    Prioridade
      ├─→ Status
      ├─→ Departamento ─→ Gerente Responsável
      ├─→ Jornada Impactada
      └─→ Paliativo

  Como usar:
    1. Selecione Prioridade → os outros dropdowns mostram
       apenas valores existentes para aquela prioridade.
    2. Selecione Departamento → Gerente mostra apenas
       gerentes daquele departamento.
    3. Todos os 10 cards se atualizam simultaneamente.
{'='*55}
""")
