# FlowETL

> Pipeline automatizado **Excel → SQL Server → Metabase**

![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)
![SQL Server](https://img.shields.io/badge/SQL%20Server-2019%2B-red?logo=microsoftsqlserver)
![Metabase](https://img.shields.io/badge/Metabase-Docker-509EE3?logo=metabase)
![License](https://img.shields.io/badge/license-MIT-green)

Monitora uma pasta local, detecta arquivos Excel automaticamente, processa e insere os dados no SQL Server, e os disponibiliza para dashboards de KPI no Metabase. Em caso de falha, dispara alertas via Telegram e e-mail.

---

## Arquitetura

```
pasta_monitorada/
      │
      ├── Watcher (watchdog)         config/tabelas.json
      │         │                              │
      │         └──────────┬──────────────────┘
      │                    │
      │                  ETL
      │           (pandas + SQLAlchemy)
      │             /        \         \
      │            /          \         \
      │     Alertas       SQL Server   tb_log_etl
      │  Telegram+Email  Staging+Prod  (auditoria)
      │
      └── processados/   erros/

                    SQL Server
                          │
                    Metabase (Docker :3001)
               KPIs + Pipeline Health
```

---

## Requisitos

### Desenvolvimento local

- Python 3.10+
- SQL Server com ODBC Driver 17+
- Conta no Telegram (Bot + Chat ID)
- Conta de e-mail com SMTP habilitado (ex.: Gmail com senha de app)

### Deploy em VM (produção)

- Linux Ubuntu 22.04+
- Python 3.11+
- Docker + Docker Compose v2
- ODBC Driver 17 for SQL Server

---

## Instalação do ODBC Driver 17

### Linux (Ubuntu/Debian)

```bash
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list \
  | sudo tee /etc/apt/sources.list.d/mssql-release.list

sudo apt update
sudo ACCEPT_EULA=Y apt install -y msodbcsql17 unixodbc-dev
```

### Windows

Baixe e instale o driver em:
[Microsoft ODBC Driver 17 for SQL Server](https://learn.microsoft.com/pt-br/sql/connect/odbc/download-odbc-driver-for-sql-server)

---

## Instalação local

```bash
# 1. Clone o repositório
git clone https://github.com/Paulo-Marcelo-Git/FlowETL.git
cd FlowETL

# 2. Crie e ative o ambiente virtual
python -m venv .venv
source .venv/bin/activate        # Linux/macOS
.venv\Scripts\activate           # Windows

# 3. Instale as dependências
pip install -r requirements.txt

# 4. Configure as variáveis de ambiente
cp .env.example .env
# Edite o .env com suas credenciais reais
```

---

## Configuração do .env

```env
# SQL Server
SQL_SERVER_CONN=mssql+pyodbc://usuario:senha@servidor/banco?driver=ODBC+Driver+17+for+SQL+Server

# Telegram
TELEGRAM_TOKEN=seu_token_aqui
TELEGRAM_CHAT_ID=seu_chat_id_aqui

# Email
EMAIL_USER=bot@empresa.com
EMAIL_PASS=senha_app_aqui
EMAIL_DESTINATARIO=gerente@empresa.com
EMAIL_SMTP=smtp.gmail.com
EMAIL_PORTA=587

# Metabase (produção VM)
MB_DB_PASS=senha_segura_postgres_aqui
MB_SITE_URL=https://ip-da-vm
```

---

## Preparação do Banco de Dados

Execute os scripts SQL na seguinte ordem no SQL Server:

```bash
-- 1. Tabela de log (auditoria)
sql/003_create_log.sql

-- 2. Tabela de staging
sql/001_create_staging.sql

-- 3. Tabela de produção
sql/002_create_producao.sql

-- 4. Stored Procedure de MERGE
sql/004_stored_procedures.sql

-- 5. Views KPI para o Metabase
sql/005_views_kpi.sql
```

---

## Uso

### Iniciar o watcher (monitoramento contínuo)

```bash
python -m bot.watcher
```

O FlowETL ficará monitorando a pasta `pasta_monitorada/`. Basta depositar um arquivo `.xlsx` nela:

```
pasta_monitorada/
└── gproblemas_abril_2024.xlsx   ← ETL dispara automaticamente
```

O prefixo do arquivo (`gproblemas`) determina a tabela de destino conforme `config/tabelas.json`.

### Reprocessar arquivo com falha manualmente

```bash
# Reprocessar um arquivo específico
python scripts/reprocessar.py erros/gproblemas_abril_2024.xlsx

# Reprocessar todos os arquivos da pasta erros/
python scripts/reprocessar.py --todos
```

---

## Fluxo do ETL

```
arquivo.xlsx (pasta_monitorada/)
    │
    ├─ Lê aba "Página4"
    ├─ Remove colunas lixo (espaços em branco)
    ├─ Renomeia colunas conforme config
    ├─ Trata tipos (dt_abertura → DATE, dt_conclusao → VARCHAR)
    ├─ Remove linhas completamente vazias
    ├─ Insere na tabela staging (TRUNCATE + INSERT)
    ├─ Executa SP de MERGE (staging → produção)
    ├─ Registra na tb_log_etl
    │
    ├─ Sucesso → move para processados/YYYY-MM/
    └─ Falha   → move para erros/ + alerta Telegram + log
```

---

## Deploy em VM (produção)

### 1. Copiar o projeto

```bash
sudo cp -r FlowETL/ /opt/flowetl
sudo useradd -r -s /bin/false flowetl
sudo chown -R flowetl:flowetl /opt/flowetl
```

### 2. Ambiente virtual e dependências

```bash
cd /opt/flowetl
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### 3. Configurar o .env

```bash
cp .env.example .env
nano .env          # preencher todas as variáveis reais
chmod 600 .env     # restringir acesso ao arquivo
```

### 4. Gerar certificado SSL

```bash
mkdir -p /opt/flowetl/nginx/certs
openssl req -x509 -nodes -days 825 -newkey rsa:2048 \
  -keyout /opt/flowetl/nginx/certs/nginx.key \
  -out    /opt/flowetl/nginx/certs/nginx.crt \
  -subj "/CN=flowetl-vm/O=Empresa/C=BR"
chmod 600 /opt/flowetl/nginx/certs/nginx.key
```

> Para VM com domínio público, use o Let's Encrypt:
> `sudo certbot certonly --standalone -d seu.dominio.com`

### 5. Subir Metabase + nginx via Docker Compose

```bash
docker compose up -d

# Verificar saúde
docker compose ps
docker compose logs -f metabase
```

Acesse `https://ip-da-vm` para o setup inicial do Metabase (aceite o certificado auto-assinado).

### 6. Instalar como serviço systemd

```bash
sudo cp flowetl.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable flowetl
sudo systemctl start flowetl

# Verificar status e logs
sudo systemctl status flowetl
sudo journalctl -u flowetl -f
```

### 7. Compartilhar pasta via Samba (opcional)

Permite que usuários Windows depositem `.xlsx` diretamente pela rede:

```bash
sudo apt install samba -y
```

Adicione ao `/etc/samba/smb.conf`:

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
# Logs em tempo real
sudo journalctl -u flowetl -f

# Reprocessar arquivos com falha
sudo -u flowetl /opt/flowetl/.venv/bin/python scripts/reprocessar.py --todos

# Reiniciar após mudança de configuração
sudo systemctl restart flowetl

# Status do Metabase
docker compose ps
docker compose logs metabase --tail=50
```

---

## Conectando o Metabase ao SQL Server

Após o setup inicial do Metabase em `https://ip-da-vm`:

1. Acesse **Admin → Databases → Add database**
2. Selecione **SQL Server**
3. Preencha:
   - **Host**: endereço do SQL Server
   - **Port**: 1433
   - **Database**: nome do banco
   - **Username / Password**: credenciais
4. Clique em **Save**

As views KPI (`vw_problemas_*`, `vw_pipeline_health`) estarão disponíveis automaticamente para criação de perguntas e dashboards.

---

## Estrutura de Pastas

```
flowetl/
├── bot/
│   ├── alertas.py      # Telegram + Email via APScheduler
│   ├── database.py     # Conexão SQL Server, staging, MERGE
│   ├── etl.py          # Lógica central do ETL
│   ├── logger.py       # Logger arquivo + banco
│   └── watcher.py      # Watchdog da pasta monitorada
├── config/
│   └── tabelas.json    # Mapeamento prefixo → tabela
├── nginx/
│   └── metabase.conf   # Proxy reverso HTTPS para o Metabase
├── scripts/
│   └── reprocessar.py  # CLI para reprocessamento manual
├── sql/
│   ├── 001_create_staging.sql
│   ├── 002_create_producao.sql
│   ├── 003_create_log.sql
│   ├── 004_stored_procedures.sql
│   └── 005_views_kpi.sql
├── pasta_monitorada/   # Deposite os .xlsx aqui
├── processados/        # Arquivos processados com sucesso
├── erros/              # Arquivos que falharam
├── docker-compose.yml  # Metabase + PostgreSQL + nginx
├── flowetl.service     # Systemd unit para o watcher
├── .env.example        # Modelo de variáveis de ambiente
└── requirements.txt
```

---

## Alertas

| Evento | Canal | Quando |
|---|---|---|
| Falha no processamento | Telegram | Imediato |
| Sucesso no processamento | Telegram | Imediato |
| Resumo de execuções | E-mail | Diariamente às 8h |

---

## Views KPI disponíveis no Metabase

| View | Descrição |
|---|---|
| `vw_problemas_por_status` | Contagem por status |
| `vw_problemas_por_prioridade` | Contagem por prioridade (P1–P4) |
| `vw_problemas_por_gerente` | Contagem por gerente responsável |
| `vw_problemas_por_departamento` | Contagem por departamento relator |
| `vw_problemas_por_sistema` | Contagem por sistema impactado |
| `vw_problemas_abertos` | Problemas não resolvidos/cancelados |
| `vw_pipeline_health` | Métricas de saúde do pipeline (tb_log_etl) |

---

## Adicionando novos tipos de planilha

1. Edite `config/tabelas.json` e adicione um novo prefixo com suas colunas
2. Crie as tabelas `stg_*` e `tb_*` no SQL Server
3. Adicione a SP de MERGE correspondente
4. O watcher detecta automaticamente pelo prefixo do arquivo

---

## Licença

MIT License — sinta-se livre para usar, modificar e distribuir.
