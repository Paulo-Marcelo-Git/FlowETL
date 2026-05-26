"""
Apaga o dashboard antigo e todos os cards obsoletos, depois recria tudo
via dashboard_executivo.py (com filtros em cascata já embutidos).

Dashboards removidos:
  - "KPI — Governança de Problemas TI"   (versão antiga)
  - "Governança TI — Dashboard Executivo" (garante recriação limpa)

Cards removidos: todos que NÃO comecem com "[Exec]".

Uso:
    python scripts/recriar_dashboard.py
"""

import os
import subprocess
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

if not ADMIN_EMAIL or not ADMIN_SENHA:
    print("❌ Erro: MB_ADMIN_USER e MB_ADMIN_PASS devem estar definidas no .env")
    sys.exit(1)

DASHBOARDS_PARA_APAGAR = [
    "KPI — Governança de Problemas TI",
    "Governança TI — Dashboard Executivo",
]


def get(path, hdr):
    return requests.get(f"{BASE}{path}", headers=hdr, verify=False)

def delete(path, hdr):
    return requests.delete(f"{BASE}{path}", headers=hdr, verify=False)

def step(msg):
    print(f"\n{'─'*55}\n▶  {msg}")


# ── 1. Login ──────────────────────────────────────────────────────────────────
step("Login no Metabase")
r = requests.post(
    f"{BASE}/api/session",
    json={"username": ADMIN_EMAIL, "password": ADMIN_SENHA},
    verify=False,
)
if r.status_code != 200:
    print(f"Erro no login: {r.text}")
    sys.exit(1)
HDR = {"X-Metabase-Session": r.json()["id"], "Content-Type": "application/json"}
print("   OK")


# ── 2. Apagar dashboards ──────────────────────────────────────────────────────
step("Removendo dashboards antigos")
r = get("/api/dashboard", HDR)
todos = r.json() if isinstance(r.json(), list) else r.json().get("data", [])

for nome in DASHBOARDS_PARA_APAGAR:
    dash = next((d for d in todos if d.get("name") == nome), None)
    if not dash:
        print(f"   [não encontrado] {nome}")
        continue
    resp = delete(f"/api/dashboard/{dash['id']}", HDR)
    if resp.status_code in (200, 204):
        print(f"   [removido] {nome} (ID {dash['id']})")
    else:
        print(f"   [erro] {nome}: {resp.text[:150]}")


# ── 3. Apagar cards obsoletos ─────────────────────────────────────────────────
step("Removendo cards sem prefixo [Exec]")
r = get("/api/card", HDR)
todos_cards = r.json() if isinstance(r.json(), list) else r.json().get("data", [])

removidos = 0
for card in todos_cards:
    nome = card.get("name", "")
    if nome.startswith("[Exec]"):
        continue
    resp = delete(f"/api/card/{card['id']}", HDR)
    if resp.status_code in (200, 204):
        print(f"   [removido] {nome} (ID {card['id']})")
        removidos += 1
    else:
        print(f"   [erro] {nome}: {resp.text[:100]}")

print(f"   Total removido: {removidos} card(s)")


# ── 4. Recriar tudo ───────────────────────────────────────────────────────────
step("Recriando dashboard com filtros em cascata")
script = Path(__file__).parent / "dashboard_executivo.py"
result = subprocess.run(
    [sys.executable, str(script)],
    capture_output=False,
)
sys.exit(result.returncode)
