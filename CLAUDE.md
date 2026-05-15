# FlowETL

> Pipeline automatizado Excel → SQL Server → Metabase

---

## 🎯 Objetivo do Projeto

O FlowETL monitora uma pasta local, detecta arquivos Excel automaticamente,
processa e insere os dados no SQL Server, e os disponibiliza para dashboards
de KPI no Metabase. Em caso de falha, dispara alertas via Telegram e Email.

---

## 🏗️ Arquitetura

### Desenvolvimento local (WSL2)

```
📁 pasta_monitorada/
      │
      ├── 👁️ Watcher (watchdog/PollingObserver)   ⚙️ config/tabelas.json
      │         │                                        │
      │         └──────────┬────────────────────────────┘
      │                    │
      │                🔄 ETL
      │      (pandas + SQLAlchemy + sincronizar_colunas)
      │             /         \           \
      │            /           \           \
      │     🚨 Alertas    🗄️ SQL Server   📋 tb_log_etl
      │  Telegram+Email   Staging+Prod    (auditoria ETL)
      │      + monitor_metabase → tb_log_metabase
      │
      └── processados/YYYY-MM/    erros/

                    🗄️ SQL Server (Docker: flowetl-sqlserver)
                          │
                    📊 Metabase (Docker: metabase:3000)
               KPIs + Pipeline Health
                          │
                    🔒 nginx (Docker :80/:443)
               Proxy reverso HTTPS
```

### Deploy em VM (produção)

```
VM Linux
│
├── systemd: flowetl.service          ← watcher Python como daemon
│     └── /opt/flowetl/
│           ├── bot/watcher.py
│           └── pasta_monitorada/     ← compartilhada via Samba
│
├── docker-compose.yml                ← 4 serviços
│     ├── sqlserver  (:1433)          ← SQL Server 2022 Developer
│     ├── nginx      (:80, :443)      ← proxy reverso HTTPS
│     ├── metabase   (interno)        ← dashboard KPI
│     └── postgres   (interno)        ← banco interno do Metabase
│
└── SQL Server (container flowetl-sqlserver)
      ├── HML-DBFLOWETL01             ← banco homologação
      └── PRD-DBFLOWETL01             ← banco produção (descomentado no sql/000)
            ├── stg_problemas_gov_ti  ← staging
            ├── tb_problemas_gov_ti   ← produção
            ├── tb_log_etl            ← auditoria do ETL
            └── tb_log_metabase       ← erros capturados do Metabase

Acesso externo: https://ip-da-vm  →  nginx  →  metabase:3000
```

---

## 🗂️ Estrutura de Pastas

```
flowetl/
│
├── CLAUDE.md                         # Este arquivo
├── CHANGELOG.md                      # Histórico de versões
├── README.md                         # Instruções de uso
├── .env                              # Credenciais reais (não subir no Git)
├── .env.example                      # Modelo sem valores reais
├── .gitignore
├── requirements.txt                  # Dependências Python
├── docker-compose.yml                # SQL Server + Metabase + PostgreSQL + nginx
├── flowetl.service                   # Systemd unit para o watcher (produção VM)
│
├── .github/
│   └── agents/
│       └── flowetl.agent.md          # Configuração do GitHub Agent
│
├── nginx/
│   ├── metabase.conf                 # Proxy reverso HTTPS (TLSv1.2+, WebSocket)
│   └── certs/                        # Certificados SSL (gerados na VM, não versionados)
│
├── config/
│   └── tabelas.json                  # Mapeamento prefixo → tabela + chave + regras
│
├── bot/
│   ├── __init__.py
│   ├── watcher.py                    # PollingObserver (compatível WSL2); dispara ETL
│   ├── etl.py                        # Lê xlsx, limpa, sincroniza schema, MERGE
│   ├── database.py                   # Engine singleton, staging, MERGE, sincronizar_colunas
│   ├── alertas.py                    # Telegram (falha+sucesso) + Email diário + APScheduler
│   ├── logger.py                     # RotatingFileHandler + tb_log_etl
│   └── monitor_metabase.py           # Lê docker logs, grava tb_log_metabase, alerta Telegram
│
├── scripts/
│   ├── reprocessar.py                # CLI: reprocessar arquivo específico ou --todos
│   ├── watcher.sh                    # Gerencia watcher em dev (start/stop/restart/status/logs)
│   ├── setup_metabase.py             # Configura conexão SQL Server no Metabase via API
│   ├── criar_dashboard_metabase.py   # Cria cards e dashboard no Metabase via API
│   ├── dashboard_executivo.py        # Dashboard Executivo · Gestão de Problemas
│   ├── dashboard_govti.py            # Variante de dashboard GovTI
│   ├── recriar_dashboard.py          # Recria dashboard limpando cards órfãos
│   ├── configurar_filtros_dashboard.py  # Adiciona filtros ao dashboard
│   └── filtros_cascata_dashboard.py  # Filtros em cascata (status → gerente etc.)
│
├── sql/
│   ├── 000_create_database.sql       # Cria HML-DBFLOWETL01 (e PRD comentado)
│   ├── 001_create_staging.sql        # stg_problemas_gov_ti
│   ├── 002_create_producao.sql       # tb_problemas_gov_ti + índices
│   ├── 003_create_log.sql            # tb_log_etl (auditoria do ETL)
│   ├── 004_stored_procedures.sql     # sp_merge_problemas_gov_ti
│   ├── 005_views_kpi.sql             # 9 views KPI
│   └── 006_create_log_metabase.sql   # tb_log_metabase (erros do Metabase)
│
├── logs/                             # Gerado em runtime (não versionar conteúdo)
│   ├── flowetl.log                   # RotatingFileHandler (10 MB, 5 backups)
│   ├── watcher.log                   # stdout/stderr do watcher em dev
│   └── watcher.pid                   # PID do watcher gerenciado por watcher.sh
│
├── pasta_monitorada/                 # Usuário deposita .xlsx aqui
│   └── .gitkeep
├── processados/                      # Arquivos processados → subpasta YYYY-MM/
│   └── .gitkeep
└── erros/                            # Arquivos que falharam
    └── .gitkeep
```

---

## ⚙️ config/tabelas.json

```json
{
  "gproblemas": {
    "tabela": "tb_problemas_gov_ti",
    "chave": "numero",
    "descricao": "Governança de Problemas TI",
    "aba_excel": "Página4",
    "colunas_ignorar": [" ", " .1", " .2", " .3", " .4", " .5", " .6", " .7", " .8", " .9", " .10"],
    "colunas_renomear": {
      "Número": "numero",
      "Prioridade": "prioridade",
      "Descrição": "descricao",
      "Data": "dt_abertura",
      "Status": "status",
      "Data de conclusão": "dt_conclusao",
      "Gerente responsável": "gerente_responsavel",
      "Departamento relator": "departamento_relator",
      "Jornada impactada": "jornada_impactada",
      "Título": "titulo",
      "Sistema": "sistema",
      "Paliativo": "paliativo",
      "Impacto": "impacto",
      "Status 14/04": "status_14_04",
      "Status 17/04": "status_17_04",
      "Status 22/04": "status_22_04",
      "Status 27/04": "status_27_04"
    }
  }
}
```

> ⚠️ Nomenclatura do arquivo: deve começar com o prefixo definido no JSON.
> Exemplo: `gproblemas_abril_2024.xlsx` → tabela `tb_problemas_gov_ti`
>
> Novas colunas de status histórico (ex: `Status 02/05`) são detectadas automaticamente
> pelo ETL via `sincronizar_colunas` e adicionadas ao banco sem intervenção manual.

---

## 🗄️ Banco de Dados

### Nomenclatura de ambientes

| Ambiente | Nome do banco |
|---|---|
| Homologação | `HML-DBFLOWETL01` |
| Produção | `PRD-DBFLOWETL01` |

Criados pelo script `sql/000_create_database.sql` (conectar ao master como SA).

### tb_problemas_gov_ti (produção)

| Coluna | Tipo SQL Server | Observação |
|---|---|---|
| `numero` | INT | PK / chave de negócio para MERGE |
| `prioridade` | VARCHAR(5) | P1, P2, P3, P4 |
| `titulo` | VARCHAR(500) | |
| `descricao` | VARCHAR(MAX) | |
| `dt_abertura` | DATE | Data de abertura do problema |
| `dt_conclusao` | VARCHAR(100) | ⚠️ Inconsistente — sempre VARCHAR |
| `status` | VARCHAR(100) | Novo, Investigação, Validação, Resolvido, etc. |
| `gerente_responsavel` | VARCHAR(200) | |
| `departamento_relator` | VARCHAR(200) | |
| `jornada_impactada` | VARCHAR(200) | |
| `sistema` | VARCHAR(200) | |
| `paliativo` | VARCHAR(10) | 'Sim' ou 'Não' |
| `impacto` | VARCHAR(MAX) | |
| `status_14_04` | VARCHAR(MAX) | Histórico de atualização |
| `status_17_04` | VARCHAR(MAX) | Histórico de atualização |
| `status_22_04` | VARCHAR(MAX) | Histórico de atualização |
| `status_27_04` | VARCHAR(MAX) | Histórico de atualização |
| `nm_arquivo_origem` | VARCHAR(500) | Nome do arquivo xlsx fonte |
| `dt_insert` | DATETIME | DEFAULT GETDATE() |
| `dt_atualizacao` | DATETIME | Atualizado no MERGE |

> Colunas de status histórico crescem semanalmente. Novas colunas detectadas pelo ETL
> são adicionadas via `ALTER TABLE` automaticamente (ver `sincronizar_colunas`).
> A SP de MERGE é recriada automaticamente após cada alteração de schema.

### stg_problemas_gov_ti (staging)

Mesmas colunas da tabela de produção, sem PK, sem `dt_insert`/`dt_atualizacao`.
Truncada e recarregada a cada execução.

### tb_log_etl (auditoria do ETL)

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_log` | INT IDENTITY | PK |
| `nm_arquivo` | VARCHAR(500) | Nome do arquivo processado |
| `nm_tabela_destino` | VARCHAR(200) | Tabela que recebeu os dados |
| `dt_processamento` | DATETIME | DEFAULT GETDATE() |
| `qt_linhas_recebidas` | INT | Total de linhas no xlsx |
| `qt_linhas_inseridas` | INT | Linhas inseridas/atualizadas com sucesso |
| `qt_linhas_rejeitadas` | INT | Linhas descartadas |
| `ds_status` | VARCHAR(20) | 'sucesso', 'falha', 'parcial' |
| `ds_erro` | VARCHAR(MAX) | Mensagem de erro (truncada em 4000 chars) |
| `tm_duracao_seg` | DECIMAL(10,2) | Tempo de processamento |

### tb_log_metabase (erros do Metabase)

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_log` | INT IDENTITY | PK |
| `fonte` | VARCHAR(30) | 'docker' |
| `nivel` | VARCHAR(10) | 'ERROR' ou 'WARN' |
| `modulo` | VARCHAR(200) | Módulo Metabase que gerou o log |
| `mensagem` | VARCHAR(MAX) | Texto do erro (até 4000 chars) |
| `dt_evento` | DATETIME | Timestamp da linha de log |
| `dt_insert` | DATETIME | DEFAULT GETDATE() |

---

## 🔄 Lógica do ETL — bot/etl.py

Ordem de execução para cada arquivo `.xlsx`:

1. Ler o arquivo com `pandas` (todos os campos como `dtype=str`)
2. Identificar a aba correta via `aba_excel` do config (`Página4`)
3. Dropar colunas da lista `colunas_ignorar`
4. Renomear colunas conforme `colunas_renomear`
5. Sanitizar colunas desconhecidas para `snake_case` seguro para SQL
6. Dropar linhas completamente vazias
7. Converter `dt_abertura` para DATE (erros viram NULL)
8. Manter `dt_conclusao` como VARCHAR (dados inconsistentes)
9. Garantir que `numero` seja INT (linhas inválidas descartadas)
10. Adicionar coluna `nm_arquivo_origem` com o nome do arquivo
11. **`sincronizar_colunas`**: comparar colunas do DataFrame com staging; adicionar novas via `ALTER TABLE` em staging e produção; recriar a SP de MERGE automaticamente
12. Truncar staging e fazer bulk insert
13. Executar SP de MERGE → retorna `(inseridas, atualizadas)`
14. Registrar na `tb_log_etl`
15. Mover arquivo para `processados/YYYY-MM/`
16. Enviar alerta de sucesso via Telegram
17. Em caso de erro: mover para `erros/`, registrar na `tb_log_etl`, enviar alerta de falha via Telegram

### TABELA_CONFIG em database.py

```python
TABELA_CONFIG = {
    'tb_problemas_gov_ti': {
        'staging':  'stg_problemas_gov_ti',
        'sp_merge': 'sp_merge_problemas_gov_ti',
    }
}
```

Para adicionar novo fluxo, incluir entrada aqui + entrada correspondente em `tabelas.json`.

---

## 🚨 Alertas — bot/alertas.py

### Telegram — falha imediata
```
❌ FlowETL — Falha no processamento
📄 Arquivo: gproblemas_abril_2024.xlsx
🔴 Erro: [mensagem do erro]
⏱️ Horário: 2024-04-25 14:32:00
```

### Telegram — sucesso imediato
```
✅ FlowETL — Processamento concluído
📄 Arquivo: gproblemas_abril_2024.xlsx
📊 Linhas inseridas: 28
⏱️ Horário: 2024-04-25 14:32:00
```

### Email HTML — relatório diário às 08h (BRT)
```
Assunto: ✅ FlowETL — Relatório Diário 25/04/2024
Corpo: Tabela HTML com nm_arquivo, tabela, recebidas, inseridas, rejeitadas, status
```

### APScheduler (3 jobs registrados ao iniciar watcher)

| Job | Trigger | Função |
|---|---|---|
| `relatorio_diario` | cron 08:00 BRT | Envia relatório e-mail com resumo do dia |
| `monitor_metabase` | interval 5 min | Lê logs Docker do Metabase → tb_log_metabase → Telegram se ERROR |
| `rescan_campos_metabase` | interval 5 min | Autentica na API Metabase e dispara rescan de valores de campo |

---

## 👁️ Monitor Metabase — bot/monitor_metabase.py

Captura erros do container Metabase em background:

1. Lê as últimas 2000 linhas de `docker logs metabase`
2. Filtra linhas `ERROR` e `WARN` relevantes (SQL/JDBC/queries)
3. Ignora ruído conhecido (SuggestedPromptsGenerator, notificações, etc.)
4. Agrupa stack traces associados (até 5 linhas abaixo)
5. Persiste apenas eventos novos (posteriores ao último `dt_evento` em `tb_log_metabase`)
6. Envia alerta Telegram se houver ERRORs críticos
7. `reescanear_campos_metabase()`: autentica via `/api/session`, lista bancos, POST `/api/database/{id}/rescan_values`

Variáveis necessárias: `CONTAINER_METABASE`, `MB_SITE_URL`, `MB_ADMIN_USER`, `MB_ADMIN_PASS`

---

## 🌱 Variáveis de Ambiente — .env.example

```env
# SQL Server (container Docker)
MSSQL_SA_PASSWORD=sua_senha_segura_aqui
SQL_SERVER_CONN=mssql+pyodbc://usuario:senha@servidor/banco?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes
DB_HOST=sqlserver
DB_PORT=1433
DB_NAME=nome_do_banco_aqui
DB_USER=sa

# Telegram
TELEGRAM_TOKEN=seu_token_aqui
TELEGRAM_CHAT_ID=seu_chat_id_aqui

# Email
EMAIL_USER=bot@empresa.com
EMAIL_PASS=senha_app_aqui
EMAIL_DESTINATARIO=gerente@empresa.com
EMAIL_SMTP=smtp.gmail.com
EMAIL_PORTA=587

# Metabase
MB_DB_PASS=senha_segura_postgres_aqui
MB_SITE_URL=https://ip-da-vm
MB_ADMIN_USER=admin@empresa.com
MB_ADMIN_PASS=senha_admin_metabase_aqui
MB_DB_DISPLAY_NAME=FlowETL - SQL Server
CONTAINER_METABASE=metabase
```

---

## 📦 requirements.txt

```
watchdog
pandas
openpyxl
sqlalchemy
pyodbc
requests
APScheduler
python-dotenv
```

---

## 🐳 docker-compose.yml — 4 serviços

| Serviço | Imagem | Porta | Função |
|---|---|---|---|
| `sqlserver` | `mssql/server:2022-latest` | 1433 | SQL Server Developer (dados do ETL) |
| `postgres` | `postgres:16-alpine` | interno | Banco interno do Metabase |
| `metabase` | `metabase/metabase:latest` | interno | Dashboard KPI |
| `nginx` | `nginx:alpine` | 80, 443 | Proxy reverso HTTPS com WebSocket |

- O Metabase **não expõe porta diretamente** em produção (comentar `ports: 3001:3000`)
- O nginx faz redirect HTTP→HTTPS e proxy para `metabase:3000`
- Healthchecks configurados: sqlserver e postgres aguardados antes de subir metabase
- Volumes persistentes: `sqlserver-data`, `metabase-db-data`, `metabase-plugins`

---

## ♻️ Reprocessamento

```bash
# Reprocessar um arquivo específico
python scripts/reprocessar.py erros/gproblemas_abril_2024.xlsx

# Reprocessar todos os arquivos na pasta /erros
python scripts/reprocessar.py --todos
```

### Watcher em desenvolvimento (watcher.sh)

```bash
./scripts/watcher.sh start    # inicia em background, salva PID em logs/watcher.pid
./scripts/watcher.sh stop     # encerra pelo PID salvo
./scripts/watcher.sh restart  # stop + start
./scripts/watcher.sh status   # verifica se está rodando
./scripts/watcher.sh logs     # tail -f logs/watcher.log
```

---

## 📊 Views KPI — sql/005_views_kpi.sql (9 views)

| View | Agrupamento |
|---|---|
| `vw_problemas_por_status` | por status |
| `vw_problemas_por_prioridade` | por prioridade (P1-P4) |
| `vw_problemas_por_gerente` | por gerente responsável |
| `vw_problemas_por_departamento` | por departamento relator |
| `vw_problemas_por_sistema` | por sistema impactado |
| `vw_problemas_abertos` | todos exceto 'Resolvido' e 'Cancelado' |
| `vw_pipeline_health` | métricas diárias de tb_log_etl |
| `vw_problemas_por_jornada` | por jornada impactada (qt_total + qt_abertos) |
| `vw_problemas_por_paliativo` | por paliativo Sim/Não (qt_total + qt_abertos) |

---

## 📋 Convenções de Código

- Python: `snake_case` para variáveis e funções
- SQL Server: `tb_` tabelas, `vw_` views, `sp_` stored procedures, `stg_` staging
- Nunca hardcodar credenciais — sempre usar `.env` via `python-dotenv`
- Sempre usar `try/except` em todo bloco de I/O e conexão com banco
- Sempre registrar na `tb_log_etl` ao final de cada execução (sucesso ou falha)
- Arquivos Python devem ter docstring no topo explicando o módulo
- Identificadores usados em SQL dinâmico devem ser validados com `_validar_identificador()`

---

## 🚀 Ordem de Criação do Zero

1. Estrutura de pastas e arquivos base
2. `requirements.txt` e `.env.example`
3. `config/tabelas.json`
4. `sql/000_create_database.sql` → bancos HML/PRD
5. `sql/003_create_log.sql` → tb_log_etl
6. `sql/006_create_log_metabase.sql` → tb_log_metabase
7. `sql/001_create_staging.sql` → stg_problemas_gov_ti
8. `sql/002_create_producao.sql` → tb_problemas_gov_ti + índices
9. `sql/004_stored_procedures.sql` → sp_merge_problemas_gov_ti
10. `sql/005_views_kpi.sql` → 9 views KPI
11. `bot/logger.py`
12. `bot/database.py` (incluindo TABELA_CONFIG e sincronizar_colunas)
13. `bot/monitor_metabase.py`
14. `bot/alertas.py` (APScheduler com 3 jobs)
15. `bot/etl.py`
16. `bot/watcher.py` (PollingObserver — compatível com WSL2/Samba)
17. `scripts/reprocessar.py`
18. `scripts/watcher.sh`
19. Scripts Metabase (`setup_metabase.py`, `criar_dashboard_metabase.py`, etc.)
20. `docker-compose.yml`
21. `nginx/metabase.conf`
22. `flowetl.service`
23. `README.md`

---

## 📌 Observações Importantes

- A planilha atual tem **28 registros** e colunas históricas que crescem semanalmente
- `dt_conclusao` tem dados inconsistentes (datas, textos como "Pendente - Triagem", "2º TRI") — tratar sempre como VARCHAR(100)
- Novas colunas de status histórico (ex: `status_27_04`) são detectadas pelo ETL e adicionadas automaticamente via `ALTER TABLE` + recriação da SP — **não é necessário alterar o SQL manualmente**
- A aba do Excel se chama **Página4** — sempre ler esta aba especificamente
- Colunas lixo da planilha (`" "`, `" .1"` até `" .10"`) são descartadas no ETL
- O watcher usa `PollingObserver` (não inotify) por compatibilidade com `/mnt/c/` no WSL2 e com compartilhamentos Samba
- O SQL Server roda como container Docker (`flowetl-sqlserver`), não como instância no host

---

## 🖥️ Deploy em VM — Passo a Passo

### Pré-requisitos na VM
- Linux (Ubuntu 22.04+ recomendado)
- Docker + Docker Compose v2
- Python 3.11+
- ODBC Driver 18 for SQL Server (`TrustServerCertificate=yes` na connection string)

### 1. Copiar o projeto

```bash
sudo cp -r flowetl/ /opt/flowetl
sudo useradd -r -s /bin/false flowetl
sudo chown -R flowetl:flowetl /opt/flowetl
```

### 2. Criar o ambiente virtual e instalar dependências

```bash
cd /opt/flowetl
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### 3. Configurar o .env

```bash
cp .env.example .env
chmod 600 .env
nano .env   # preencher todas as variáveis reais
```

### 4. Gerar certificado SSL (auto-assinado para rede interna)

```bash
mkdir -p /opt/flowetl/nginx/certs
openssl req -x509 -nodes -days 825 -newkey rsa:2048 \
  -keyout /opt/flowetl/nginx/certs/nginx.key \
  -out  /opt/flowetl/nginx/certs/nginx.crt \
  -subj "/CN=flowetl-vm/O=Empresa/C=BR"
chmod 600 /opt/flowetl/nginx/certs/nginx.key
```

> Se a VM tiver domínio público, use Let's Encrypt:
> `sudo certbot certonly --standalone -d seu.dominio.com`
> e atualize os caminhos em `nginx/metabase.conf`.

### 5. Subir os containers via Docker Compose

```bash
# MB_SITE_URL deve usar HTTPS no .env antes de subir
docker compose up -d

# Verificar saúde
docker compose ps
docker compose logs -f metabase
docker compose logs -f sqlserver
docker compose logs -f nginx
```

Acesse `https://ip-da-vm` para o setup inicial do Metabase (aceitar certificado auto-assinado).

### 6. Criar banco e tabelas no SQL Server

```bash
# Conectar ao container e criar banco
docker exec -it flowetl-sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -No \
  -i /path/to/sql/000_create_database.sql

# Criar tabelas e views (conectado ao banco correto)
sqlcmd -i sql/001_create_staging.sql
sqlcmd -i sql/002_create_producao.sql
sqlcmd -i sql/003_create_log.sql
sqlcmd -i sql/004_stored_procedures.sql
sqlcmd -i sql/005_views_kpi.sql
sqlcmd -i sql/006_create_log_metabase.sql
```

### 7. Instalar o FlowETL como serviço systemd

```bash
sudo cp flowetl.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable flowetl
sudo systemctl start flowetl

# Verificar status e logs
sudo systemctl status flowetl
sudo journalctl -u flowetl -f
```

### 8. Compartilhar pasta_monitorada via Samba (opcional)

```bash
sudo apt install samba -y
```

Adicionar ao `/etc/samba/smb.conf`:

```ini
[FlowETL]
path = /opt/flowetl/pasta_monitorada
browseable = yes
writable = yes
valid users = @flowetl
```

```bash
sudo smbpasswd -a flowetl
sudo systemctl restart smbd
```

Usuários Windows mapeiam `\\ip-da-vm\FlowETL` como unidade de rede.

### Comandos de operação

```bash
# Ver logs em tempo real
sudo journalctl -u flowetl -f

# Reprocessar arquivos com falha
cd /opt/flowetl
sudo -u flowetl .venv/bin/python scripts/reprocessar.py --todos

# Reiniciar após mudança de configuração
sudo systemctl restart flowetl

# Ver status dos containers
docker compose ps
docker compose logs metabase --tail=50
docker compose logs sqlserver --tail=50
```

### Notas de segurança

- `.env` deve ter permissão `600` (`chmod 600 .env`)
- O Metabase **não expõe porta diretamente** — todo acesso passa pelo nginx (:443)
- O serviço systemd roda como usuário `flowetl` sem privilégios de root (`NoNewPrivileges=true`, `PrivateTmp=true`)
- Metabase usa PostgreSQL dedicado (não H2) para garantir persistência
- Certificados SSL ficam em `nginx/certs/` — nunca versionar (estão no `.gitignore`)
- Para debug local sem nginx: descomentar `ports: ["3001:3000"]` no `docker-compose.yml`
- `MSSQL_SA_PASSWORD` deve ter mínimo 8 chars com maiúscula, minúscula, número e símbolo

### Arquivos de infraestrutura

| Arquivo | Função |
|---|---|
| `docker-compose.yml` | Orquestra SQL Server + nginx + Metabase + PostgreSQL |
| `flowetl.service` | Daemon systemd do watcher Python |
| `nginx/metabase.conf` | Proxy reverso HTTPS (TLS 1.2+, WebSocket, 50M upload) |
| `nginx/certs/` | Certificados SSL (gerados na VM, não versionados) |
