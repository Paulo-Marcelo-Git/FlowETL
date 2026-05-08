"""Script que cria o dashboard executivo de KPI no Metabase via API REST."""

import time
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://localhost"
EMAIL = "pcs@tf.com.br"
PASSWORD = "Valentina100126@"

session = requests.Session()
session.verify = False


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def autenticar():
    r = session.post(f"{BASE_URL}/api/session", json={"username": EMAIL, "password": PASSWORD})
    r.raise_for_status()
    token = r.json()["id"]
    session.headers.update({"X-Metabase-Session": token})
    print(f"[OK] Autenticado — token: {token[:8]}...")


# ---------------------------------------------------------------------------
# Banco de dados
# ---------------------------------------------------------------------------

def obter_database_id(nome_parcial="HML"):
    r = session.get(f"{BASE_URL}/api/database")
    r.raise_for_status()
    dbs = r.json().get("data", r.json())
    for db in dbs:
        if nome_parcial.upper() in db["name"].upper():
            print(f"[OK] Database: '{db['name']}' (id={db['id']})")
            return db["id"]
    for db in dbs:
        if db["engine"] not in ("h2", "postgres"):
            print(f"[OK] Database fallback: '{db['name']}' (id={db['id']})")
            return db["id"]
    raise RuntimeError(f"Nenhum database encontrado. Disponíveis: {[d['name'] for d in dbs]}")


def sincronizar_database(db_id):
    session.post(f"{BASE_URL}/api/database/{db_id}/sync_schema")
    print("[OK] Sync solicitado — aguardando 15s...")
    time.sleep(15)


def obter_tabela_id(db_id, nome_view):
    r = session.get(f"{BASE_URL}/api/database/{db_id}/metadata")
    r.raise_for_status()
    tabelas = r.json().get("tables", [])
    for t in tabelas:
        if t["name"].lower() == nome_view.lower():
            return t["id"]
    raise RuntimeError(f"View '{nome_view}' não encontrada. Disponíveis: {[t['name'] for t in tabelas]}")


def obter_field_id(table_id, field_name):
    r = session.get(f"{BASE_URL}/api/table/{table_id}/query_metadata")
    r.raise_for_status()
    for f in r.json().get("fields", []):
        if f["name"].lower() == field_name.lower():
            return f["id"]
    raise RuntimeError(f"Campo '{field_name}' não encontrado na tabela {table_id}")


# ---------------------------------------------------------------------------
# Cards
# ---------------------------------------------------------------------------

def criar_card(payload):
    r = session.post(f"{BASE_URL}/api/card", json=payload)
    r.raise_for_status()
    card = r.json()
    print(f"  [card] '{card['name']}' (id={card['id']})")
    return card["id"]


def card_total_abertos(db_id, table_id):
    return criar_card({
        "name": "Total de Problemas Abertos",
        "display": "scalar",
        "database_id": db_id,
        "dataset_query": {
            "type": "query",
            "database": db_id,
            "query": {
                "source-table": table_id,
                "aggregation": [["count"]],
            },
        },
        "visualization_settings": {},
        "collection_id": None,
    })


def card_por_coluna(db_id, table_id, nome_card, col_grupo, display="bar"):
    fid = obter_field_id(table_id, col_grupo)
    return criar_card({
        "name": nome_card,
        "display": display,
        "database_id": db_id,
        "dataset_query": {
            "type": "query",
            "database": db_id,
            "query": {
                "source-table": table_id,
                "aggregation": [["count"]],
                "breakout": [["field", fid, {"base-type": "type/Text"}]],
            },
        },
        "visualization_settings": {
            "graph.dimensions": [col_grupo],
            "graph.metrics": ["count"],
        },
        "collection_id": None,
    })


def card_pipeline_health(db_id, table_id):
    fid = obter_field_id(table_id, "dt_execucao")
    return criar_card({
        "name": "Pipeline Health — Execuções por Dia",
        "display": "line",
        "database_id": db_id,
        "dataset_query": {
            "type": "query",
            "database": db_id,
            "query": {
                "source-table": table_id,
                "aggregation": [["count"]],
                "breakout": [
                    ["field", fid, {"base-type": "type/DateTime", "temporal-unit": "day"}]
                ],
            },
        },
        "visualization_settings": {
            "graph.dimensions": ["dt_processamento"],
            "graph.metrics": ["count"],
        },
        "collection_id": None,
    })


def card_tabela_abertos(db_id, table_id):
    return criar_card({
        "name": "Lista de Problemas Abertos",
        "display": "table",
        "database_id": db_id,
        "dataset_query": {
            "type": "query",
            "database": db_id,
            "query": {"source-table": table_id},
        },
        "visualization_settings": {},
        "collection_id": None,
    })


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

def criar_dashboard(nome):
    r = session.post(f"{BASE_URL}/api/dashboard", json={"name": nome, "collection_id": None})
    r.raise_for_status()
    dash = r.json()
    print(f"[OK] Dashboard criado: '{dash['name']}' (id={dash['id']})")
    return dash["id"]


def montar_dashboard(dash_id, layout_cards, filtros):
    """
    Usa PUT /api/dashboard/{id} com dashcards (IDs negativos para novos cards).
    layout_cards = lista de dicts: card_id, row, col, size_x, size_y, param_mappings
    """
    dashcards = []
    for i, item in enumerate(layout_cards):
        dc = {
            "id": -(i + 1),          # ID temporário negativo = novo dashcard
            "card_id": item["card_id"],
            "row": item["row"],
            "col": item["col"],
            "size_x": item["size_x"],
            "size_y": item["size_y"],
            "parameter_mappings": item.get("parameter_mappings", []),
            "visualization_settings": {},
            "series": [],
        }
        dashcards.append(dc)

    payload = {
        "dashcards": dashcards,
        "parameters": filtros,
    }

    r = session.put(f"{BASE_URL}/api/dashboard/{dash_id}", json=payload)
    r.raise_for_status()
    result = r.json()
    real_dashcards = result.get("dashcards", [])
    print(f"[OK] {len(real_dashcards)} cards adicionados ao dashboard")
    return real_dashcards


def mapear_filtros(dash_id, real_dashcards, card_map, filtros, tabela_abertos_id):
    """
    Atualiza parameter_mappings de cada dashcard com os filtros adequados.
    card_map: {card_id -> nome_card}
    filtros: lista de dicts com id/slug/campo
    """
    # Mapeamento: slug -> field_id na view vw_problemas_abertos
    campo_para_filtro = {
        "status":               obter_field_id(tabela_abertos_id, "status"),
        "prioridade":           obter_field_id(tabela_abertos_id, "prioridade"),
        "gerente_responsavel":  obter_field_id(tabela_abertos_id, "gerente_responsavel"),
        "departamento_relator": obter_field_id(tabela_abertos_id, "departamento_relator"),
        "jornada_impactada":    obter_field_id(tabela_abertos_id, "jornada_impactada"),
        "paliativo":            obter_field_id(tabela_abertos_id, "paliativo"),
    }

    # Só mapeia filtros para cards que usam vw_problemas_abertos (total + tabela)
    cards_filtraveis = {card_map["total"], card_map["tabela"]}

    updated_dashcards = []
    for dc in real_dashcards:
        card_id = dc.get("card_id")
        if card_id not in cards_filtraveis:
            updated_dashcards.append({
                "id": dc["id"],
                "card_id": card_id,
                "row": dc["row"],
                "col": dc["col"],
                "size_x": dc["size_x"],
                "size_y": dc["size_y"],
                "parameter_mappings": [],
                "visualization_settings": dc.get("visualization_settings", {}),
                "series": [],
            })
            continue

        mappings = []
        for f in filtros:
            slug = f["slug"]
            if slug in campo_para_filtro:
                mappings.append({
                    "parameter_id": f["id"],
                    "card_id": card_id,
                    "target": ["dimension", ["field", campo_para_filtro[slug], {"base-type": "type/Text"}]],
                })

        updated_dashcards.append({
            "id": dc["id"],
            "card_id": card_id,
            "row": dc["row"],
            "col": dc["col"],
            "size_x": dc["size_x"],
            "size_y": dc["size_y"],
            "parameter_mappings": mappings,
            "visualization_settings": dc.get("visualization_settings", {}),
            "series": [],
        })

    r = session.put(
        f"{BASE_URL}/api/dashboard/{dash_id}",
        json={"dashcards": updated_dashcards},
    )
    r.raise_for_status()
    print(f"[OK] Filtros mapeados nos cards filtraveis")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("FlowETL — Criação do Dashboard Executivo no Metabase")
    print("=" * 60)

    autenticar()
    db_id = obter_database_id("HML")
    sincronizar_database(db_id)

    print("\n[1/3] Obtendo IDs das views...")
    t_status       = obter_tabela_id(db_id, "vw_problemas_por_status")
    t_prioridade   = obter_tabela_id(db_id, "vw_problemas_por_prioridade")
    t_gerente      = obter_tabela_id(db_id, "vw_problemas_por_gerente")
    t_departamento = obter_tabela_id(db_id, "vw_problemas_por_departamento")
    t_sistema      = obter_tabela_id(db_id, "vw_problemas_por_sistema")
    t_jornada      = obter_tabela_id(db_id, "vw_problemas_por_jornada")
    t_paliativo    = obter_tabela_id(db_id, "vw_problemas_por_paliativo")
    t_abertos      = obter_tabela_id(db_id, "vw_problemas_abertos")
    t_health       = obter_tabela_id(db_id, "vw_pipeline_health")
    print("  Todas as views localizadas.")

    print("\n[2/3] Criando cards...")
    c_total        = card_total_abertos(db_id, t_abertos)
    c_status       = card_por_coluna(db_id, t_status,       "Distribuição por Status",          "status",               "pie")
    c_prioridade   = card_por_coluna(db_id, t_prioridade,   "Distribuição por Prioridade",      "prioridade",           "bar")
    c_gerente      = card_por_coluna(db_id, t_gerente,      "Problemas por Gerente",            "gerente_responsavel",  "bar")
    c_departamento = card_por_coluna(db_id, t_departamento, "Problemas por Departamento",       "departamento_relator", "bar")
    c_jornada      = card_por_coluna(db_id, t_jornada,      "Problemas por Jornada Impactada",  "jornada_impactada",    "bar")
    c_paliativo    = card_por_coluna(db_id, t_paliativo,    "Paliativo Sim/Não",                "paliativo",            "pie")
    c_sistema      = card_por_coluna(db_id, t_sistema,      "Problemas por Sistema",            "sistema",              "bar")
    c_health       = card_pipeline_health(db_id, t_health)
    c_tabela       = card_tabela_abertos(db_id, t_abertos)

    card_map = {"total": c_total, "tabela": c_tabela}

    print("\n[3/3] Montando dashboard...")
    dash_id = criar_dashboard("KPI — Governança de Problemas TI")

    # Filtros interativos
    filtros = [
        {"id": "f_status",       "name": "Status",               "slug": "status",               "type": "string/="},
        {"id": "f_prioridade",   "name": "Prioridade",           "slug": "prioridade",            "type": "string/="},
        {"id": "f_gerente",      "name": "Gerente Responsável",  "slug": "gerente_responsavel",   "type": "string/="},
        {"id": "f_departamento", "name": "Departamento Relator", "slug": "departamento_relator",  "type": "string/="},
        {"id": "f_jornada",      "name": "Jornada Impactada",    "slug": "jornada_impactada",     "type": "string/="},
        {"id": "f_paliativo",    "name": "Paliativo",            "slug": "paliativo",             "type": "string/="},
    ]

    # Layout: grade de 24 colunas
    layout = [
        {"card_id": c_total,        "row": 0,  "col": 0,  "size_x": 6,  "size_y": 3},
        {"card_id": c_status,       "row": 0,  "col": 6,  "size_x": 9,  "size_y": 6},
        {"card_id": c_paliativo,    "row": 0,  "col": 15, "size_x": 9,  "size_y": 6},
        {"card_id": c_prioridade,   "row": 3,  "col": 0,  "size_x": 6,  "size_y": 6},
        {"card_id": c_gerente,      "row": 6,  "col": 0,  "size_x": 12, "size_y": 6},
        {"card_id": c_departamento, "row": 6,  "col": 12, "size_x": 12, "size_y": 6},
        {"card_id": c_jornada,      "row": 12, "col": 0,  "size_x": 12, "size_y": 6},
        {"card_id": c_sistema,      "row": 12, "col": 12, "size_x": 12, "size_y": 6},
        {"card_id": c_health,       "row": 18, "col": 0,  "size_x": 24, "size_y": 6},
        {"card_id": c_tabela,       "row": 24, "col": 0,  "size_x": 24, "size_y": 9},
    ]

    real_dashcards = montar_dashboard(dash_id, layout, filtros)
    mapear_filtros(dash_id, real_dashcards, card_map, filtros, t_abertos)

    print("\n" + "=" * 60)
    print("[CONCLUÍDO] Dashboard disponível em:")
    print(f"  https://localhost/dashboard/{dash_id}")
    print("=" * 60)


if __name__ == "__main__":
    main()
