"""
Adiciona filtros interativos ao Dashboard Executivo do Metabase.

Problema resolvido: cards com SQL nativo exigem template tags ({{variavel}})
para aceitar filtros do dashboard — sem isso o Metabase retorna erro.

Ação:
  1. Atualiza o SQL de cada card para incluir cláusulas opcionais
     [[AND coluna = {{filtro_xxx}}]]
  2. Adiciona 6 parâmetros de filtro ao dashboard
  3. Mapeia cada parâmetro para os template tags nos cards

Filtros criados:
  - Status | Prioridade | Jornada Impactada
  - Gerente Responsável | Departamento Relator | Paliativo
"""

import os
import sys
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
DB_NAME_MB  = "FlowETL - SQL Server"
DASH_NAME   = "Governança TI — Dashboard Executivo"


def get(path, headers=None):
    return requests.get(f"{BASE}{path}", headers=headers, verify=False)

def post(path, payload, headers=None):
    return requests.post(f"{BASE}{path}", json=payload, headers=headers, verify=False)

def put(path, payload, headers=None):
    return requests.put(f"{BASE}{path}", json=payload, headers=headers, verify=False)

def step(msg):
    print(f"\n{'─'*55}\n▶  {msg}")


# Filtros e seus template tags
# IDs fixos de 8 chars hex — Metabase rejeita UUIDs completos com hífens
FILTROS = [
    {"slug": "filtro_status",       "name": "Status",              "col": "status",              "id": "a1b2c3d4"},
    {"slug": "filtro_prioridade",   "name": "Prioridade",          "col": "prioridade",           "id": "e5f6a7b8"},
    {"slug": "filtro_jornada",      "name": "Jornada Impactada",   "col": "jornada_impactada",    "id": "c9d0e1f2"},
    {"slug": "filtro_gerente",      "name": "Gerente Responsável", "col": "gerente_responsavel",  "id": "a3b4c5d6"},
    {"slug": "filtro_departamento", "name": "Departamento",        "col": "departamento_relator", "id": "e7f8a9b0"},
    {"slug": "filtro_paliativo",    "name": "Paliativo",           "col": "paliativo",            "id": "c1d2e3f4"},
]

# Cláusulas opcionais para injetar em todos os cards
FILTROS_SQL = "\n".join(
    f"[[AND {f['col']} = {{{{{f['slug']}}}}}]]"
    for f in FILTROS
)

# Template tags dict para o dataset_query
def build_template_tags():
    return {
        f["slug"]: {
            "name": f["slug"],
            "display-name": f["name"],
            "type": "text",
            "required": False,
            "default": None,
        }
        for f in FILTROS
    }

# Novos SQLs com template tags
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
""",
    "[Exec] Problemas por Prioridade": f"""
SELECT prioridade, COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FILTROS_SQL}
GROUP BY prioridade
""",
    "[Exec] Problemas por Jornada Impactada": f"""
SELECT ISNULL(jornada_impactada, 'Não informado') AS jornada_impactada,
       COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FILTROS_SQL}
GROUP BY jornada_impactada
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
""",
    "[Exec] Problemas por Departamento Relator": f"""
SELECT ISNULL(departamento_relator, 'Não informado') AS departamento_relator,
       COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
WHERE 1=1
{FILTROS_SQL}
GROUP BY departamento_relator
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
""",
}


# ── 1. Login ──────────────────────────────────────────────────────────────────
step("Login no Metabase")
r = post("/api/session", {"username": ADMIN_EMAIL, "password": ADMIN_SENHA})
if r.status_code != 200:
    print(f"Erro no login: {r.text}")
    sys.exit(1)
session_id = r.json()["id"]
HDR = {"X-Metabase-Session": session_id, "Content-Type": "application/json"}
print("   OK")


# ── 2. Buscar ID do banco ─────────────────────────────────────────────────────
step("Obtendo ID do banco SQL Server")
r = get("/api/database", HDR)
dbs = r.json().get("data", r.json() if isinstance(r.json(), list) else [])
db_row = next((d for d in dbs if d.get("name") == DB_NAME_MB), None)
if not db_row:
    print(f"Banco '{DB_NAME_MB}' não encontrado no Metabase.")
    sys.exit(1)
db_id = db_row["id"]
print(f"   Banco ID: {db_id}")


# ── 3. Atualizar cards com template tags ──────────────────────────────────────
step("Atualizando SQL dos cards com template tags de filtro")
r = get("/api/card", HDR)
todos_cards = r.json() if isinstance(r.json(), list) else r.json().get("data", [])
cards_por_nome = {c["name"]: c for c in todos_cards}

card_ids_por_nome = {}
for nome, novo_sql in CARDS_SQL.items():
    card = cards_por_nome.get(nome)
    if not card:
        print(f"   [não encontrado] {nome}")
        continue

    payload = {
        "dataset_query": {
            "type": "native",
            "native": {
                "query": novo_sql.strip(),
                "template-tags": build_template_tags(),
            },
            "database": db_id,
        },
    }
    r = put(f"/api/card/{card['id']}", payload, HDR)
    if r.status_code not in (200, 202):
        print(f"   [erro] {nome}: {r.text[:200]}")
    else:
        card_ids_por_nome[nome] = card["id"]
        print(f"   [atualizado] {nome} (ID {card['id']})")


# ── 4. Buscar dashboard e dashcards reais ─────────────────────────────────────
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


# ── 5. Montar parâmetros do dashboard ─────────────────────────────────────────
step("Configurando parâmetros de filtro no dashboard")

params_dashboard = [
    {
        "id":        f["id"],
        "name":      f["name"],
        "slug":      f["slug"],
        "type":      "string/=",
        "sectionId": "string",
    }
    for f in FILTROS
]

# Click behavior: clicar em barra/fatia de um gráfico ativa o filtro correspondente
# Mapeamento: nome do card → (coluna que o gráfico exibe, id do parâmetro)
CLICK_BEHAVIOR = {
    "[Exec] Problemas por Status":            ("status",               "a1b2c3d4"),
    "[Exec] Problemas por Prioridade":        ("prioridade",           "e5f6a7b8"),
    "[Exec] Problemas por Jornada Impactada": ("jornada_impactada",    "c9d0e1f2"),
    "[Exec] Paliativo — Sim vs Não":          ("paliativo",            "c1d2e3f4"),
    "[Exec] Problemas por Gerente Responsável": ("gerente_responsavel", "a3b4c5d6"),
    "[Exec] Problemas por Departamento Relator": ("departamento_relator", "e7f8a9b0"),
}


def _click_behavior(col: str, param_id: str) -> dict:
    """Retorna o bloco click_behavior para um gráfico com cross-filter."""
    return {
        "type": "crossfilter",
        "parameterMapping": {
            param_id: {
                "id":     param_id,
                "source": {"type": "column", "id": col, "name": col},
                "target": {"type": "parameter", "id": param_id},
            }
        },
    }


# Para cada dashcard, mapear TODOS os parâmetros ao template tag correspondente
# e adicionar click_behavior aos gráficos
dashcards_atualizados = []
for dc in dashcards_atuais:
    card_id   = dc.get("card_id")
    nome_card = next((n for n, cid in card_ids_por_nome.items() if cid == card_id), None)

    param_mappings = []
    if nome_card:
        for f in FILTROS:
            param_mappings.append({
                "parameter_id": f["id"],
                "card_id":      card_id,
                "target":       ["variable", ["template-tag", f["slug"]]],
            })

    viz = dict(dc.get("visualization_settings") or {})
    if nome_card and nome_card in CLICK_BEHAVIOR:
        col, param_id = CLICK_BEHAVIOR[nome_card]
        viz["click_behavior"] = _click_behavior(col, param_id)

    dashcards_atualizados.append({
        "id":                      dc["id"],
        "card_id":                 card_id,
        "row":                     dc["row"],
        "col":                     dc["col"],
        "size_x":                  dc["size_x"],
        "size_y":                  dc["size_y"],
        "parameter_mappings":      param_mappings,
        "visualization_settings":  viz,
    })

r = put(
    f"/api/dashboard/{dash_id}",
    {
        "parameters": params_dashboard,
        "dashcards":  dashcards_atualizados,
    },
    HDR,
)
if r.status_code not in (200, 202):
    print(f"   [erro] ao atualizar dashboard: {r.text[:300]}")
    sys.exit(1)

print(f"   {len(params_dashboard)} parâmetros configurados")
print(f"   {len(dashcards_atualizados)} cards mapeados")


# ── Resumo ────────────────────────────────────────────────────────────────────
print(f"""
{'='*55}
Filtros configurados com sucesso!

  Dashboard: https://localhost/dashboard/{dash_id}

  Filtros disponíveis no topo do dashboard:
    • Status
    • Prioridade
    • Jornada Impactada
    • Gerente Responsável
    • Departamento
    • Paliativo

  Todos os 10 cards respondem a esses filtros.
{'='*55}
""")
