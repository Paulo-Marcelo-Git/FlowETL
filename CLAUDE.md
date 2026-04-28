# FlowETL

> Pipeline automatizado Excel → SQL Server → Metabase

---

## 🎯 Objetivo do Projeto

O FlowETL monitora uma pasta local, detecta arquivos Excel automaticamente,
processa e insere os dados no SQL Server, e os disponibiliza para dashboards
de KPI no Metabase. Em caso de falha, dispara alertas via Telegram e Email.

---

## 🏗️ Arquitetura

### Desenvolvimento local

```
📁 pasta_monitorada/
      │
      ├── 👁️ Watcher (watchdog)         ⚙️ config/tabelas.json
      │         │                              │
      │         └──────────┬─────────────────-┘
      │                    │
      │                🔄 ETL
      │           (pandas + SQLAlchemy)
      │             /        \         \
      │            /          \         \
      │     🚨 Alertas   🗄️ SQL Server  📋 tb_log_etl
      │   Telegram+Email  Staging+Prod   (auditoria)
      │
      └── processados/   erros/

                    🗄️ SQL Server
                          │
                    📊 Metabase (Docker :3001)
               KPIs + Pipeline Health
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
├── docker-compose.yml
│     ├── nginx         (:80, :443)   ← proxy reverso HTTPS
│     ├── metabase      (interno)     ← dashboard KPI
│     └── postgres      (interno)     ← banco interno do Metabase
│
└── SQL Server (host ou rede)
      ├── stg_problemas_gov_ti        ← staging
      ├── tb_problemas_gov_ti         ← produção
      └── tb_log_etl                  ← auditoria

Acesso externo: https://ip-da-vm  →  nginx  →  metabase:3000
```

---

## 🗂️ Estrutura de Pastas — CRIAR EXATAMENTE ASSIM

```
flowetl/
│
├── CLAUDE.md                        # Este arquivo
├── CHANGELOG.md                     # Histórico de versões
├── README.md                        # Instruções de uso
├── .env                             # Credenciais reais (não subir no Git)
├── .env.example                     # Modelo sem valores reais
├── .gitignore                       # Ignorar .env, __pycache__, *.pyc, .venv, pasta_monitorada/*.xlsx
├── requirements.txt                 # Dependências Python
├── docker-compose.yml               # Metabase + PostgreSQL + nginx (produção VM)
├── flowetl.service                  # Systemd unit para o watcher (produção VM)
│
├── nginx/
│   ├── metabase.conf                # Proxy reverso HTTPS para o Metabase
│   └── certs/                       # Certificados SSL (gerados na VM, não versionados)
│
├── config/
│   └── tabelas.json                 # Mapeamento prefixo → tabela + chave + regras
│
├── bot/
│   ├── __init__.py
│   ├── watcher.py                   # Monitora a pasta, dispara ETL
│   ├── etl.py                       # Lê xlsx, valida, limpa, chama database
│   ├── database.py                  # Conexão SQL Server, upsert via MERGE
│   ├── alertas.py                   # Telegram (imediato) + Email (diário)
│   └── logger.py                    # Log estruturado em arquivo e banco
│
├── scripts/
│   └── reprocessar.py               # CLI: python reprocessar.py erros/arquivo.xlsx
│
├── sql/
│   ├── 001_create_staging.sql       # Tabela staging (dados brutos)
│   ├── 002_create_producao.sql      # Tabela produção + colunas de controle
│   ├── 003_create_log.sql           # Tabela tb_log_etl (auditoria)
│   ├── 004_stored_procedures.sql    # SP de MERGE staging → produção
│   └── 005_views_kpi.sql            # Views consumidas pelo Metabase
│
├── pasta_monitorada/                # Usuário deposita .xlsx aqui
│   └── .gitkeep
├── processados/                     # Arquivos processados com sucesso
│   └── .gitkeep
└── erros/                           # Arquivos que falharam
    └── .gitkeep
```

---

## ⚙️ config/tabelas.json — CRIAR COM ESTE CONTEÚDO

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
      "Status 22/04": "status_22_04"
    }
  }
}
```

> ⚠️ Nomenclatura do arquivo: deve começar com o prefixo definido no JSON.
> Exemplo: `gproblemas_abril_2024.xlsx` → tabela `tb_problemas_gov_ti`

---

## 🗄️ Banco de Dados — Estrutura da Tabela Principal

### tb_problemas_gov_ti (produção)

| Coluna | Tipo SQL Server | Observação |
|---|---|---|
| `numero` | INT | PK / chave de negócio para MERGE |
| `prioridade` | VARCHAR(5) | P1, P2, P3, P4 |
| `titulo` | VARCHAR(500) | |
| `descricao` | VARCHAR(MAX) | Texto longo |
| `dt_abertura` | DATE | Data de abertura do problema |
| `dt_conclusao` | VARCHAR(100) | ⚠️ Inconsistente na planilha — tratar como texto |
| `status` | VARCHAR(100) | Novo, Investigação, Validação, Resolvido, etc. |
| `gerente_responsavel` | VARCHAR(200) | |
| `departamento_relator` | VARCHAR(200) | |
| `jornada_impactada` | VARCHAR(200) | |
| `sistema` | VARCHAR(200) | |
| `paliativo` | VARCHAR(10) | 'Sim' ou 'Não' |
| `impacto` | VARCHAR(MAX) | Texto longo |
| `status_14_04` | VARCHAR(MAX) | Histórico de atualização |
| `status_17_04` | VARCHAR(MAX) | Histórico de atualização |
| `status_22_04` | VARCHAR(MAX) | Histórico de atualização |
| `dt_insert` | DATETIME | DEFAULT GETDATE() |
| `dt_atualizacao` | DATETIME | Atualizado no MERGE |
| `nm_arquivo_origem` | VARCHAR(500) | Nome do arquivo xlsx fonte |

### tb_log_etl (auditoria de cada execução)

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_log` | INT IDENTITY | PK |
| `nm_arquivo` | VARCHAR(500) | Nome do arquivo processado |
| `nm_tabela_destino` | VARCHAR(200) | Tabela que recebeu os dados |
| `dt_processamento` | DATETIME | DEFAULT GETDATE() |
| `qt_linhas_recebidas` | INT | Total de linhas no xlsx |
| `qt_linhas_inseridas` | INT | Linhas inseridas com sucesso |
| `qt_linhas_rejeitadas` | INT | Linhas com erro ou duplicata |
| `ds_status` | VARCHAR(20) | 'sucesso', 'falha', 'parcial' |
| `ds_erro` | VARCHAR(MAX) | Mensagem de erro se houver |
| `tm_duracao_seg` | DECIMAL(10,2) | Tempo de processamento |

---

## 🔄 Lógica do ETL — bot/etl.py

O ETL deve seguir exatamente esta ordem:

1. Ler o arquivo `.xlsx` com `pandas`
2. Identificar a aba correta via `aba_excel` do config (Página4)
3. Dropar colunas da lista `colunas_ignorar`
4. Renomear colunas conforme `colunas_renomear`
5. Tratar `dt_conclusao` como VARCHAR (não converter para data — está inconsistente)
6. Tratar `dt_abertura` como DATE
7. Dropar linhas completamente vazias
8. Inserir na tabela de **staging** primeiro
9. Executar a **Stored Procedure de MERGE** staging → produção usando `numero` como chave
10. Registrar resultado na **tb_log_etl**
11. Mover arquivo para `/processados/YYYY-MM/` com sucesso
12. Em caso de erro: mover para `/erros/`, disparar alerta Telegram + Email

---

## 🚨 Alertas — bot/alertas.py

### Telegram (disparo imediato em caso de falha)
```
❌ FlowETL — Falha no processamento
📄 Arquivo: gproblemas_abril_2024.xlsx
🔴 Erro: [mensagem do erro]
⏱️ Horário: 2024-04-25 14:32:00
```

### Email (relatório diário às 8h)
```
Assunto: ✅ FlowETL — Relatório Diário 25/04/2024
Corpo: Resumo de arquivos processados, linhas inseridas e falhas
```

---

## 🌱 Variáveis de Ambiente — .env.example

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

# Metabase (docker-compose — produção VM)
MB_DB_PASS=senha_segura_postgres_aqui
MB_SITE_URL=http://ip-da-vm:3001
```

---

## 📦 requirements.txt — CRIAR COM ESTE CONTEÚDO

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

## ♻️ Reprocessamento — scripts/reprocessar.py

```bash
# Reprocessar um arquivo específico
python scripts/reprocessar.py erros/gproblemas_abril_2024.xlsx

# Reprocessar todos os arquivos na pasta /erros
python scripts/reprocessar.py --todos
```

---

## 📊 Views KPI para o Metabase — sql/005_views_kpi.sql

Criar as seguintes views:

1. `vw_problemas_por_status` — contagem de problemas agrupados por status
2. `vw_problemas_por_prioridade` — contagem por prioridade (P1, P2, P3, P4)
3. `vw_problemas_por_gerente` — contagem por gerente responsável
4. `vw_problemas_por_departamento` — contagem por departamento relator
5. `vw_problemas_por_sistema` — contagem por sistema impactado
6. `vw_problemas_abertos` — todos os problemas com status diferente de 'Resolvido' e 'Cancelado'
7. `vw_pipeline_health` — métricas do tb_log_etl para o dashboard de saúde

---

## 📋 Convenções de Código

- Python: `snake_case` para variáveis e funções
- SQL Server: `tb_` tabelas, `vw_` views, `sp_` stored procedures, `stg_` staging
- Nunca hardcodar credenciais — sempre usar `.env` via `python-dotenv`
- Sempre usar `try/except` em todo bloco de I/O e conexão com banco
- Sempre registrar na `tb_log_etl` ao final de cada execução (sucesso ou falha)
- Arquivos Python devem ter docstring no topo explicando o módulo

---

## 🚀 Ordem de Criação — SIGA EXATAMENTE ESTA SEQUÊNCIA

1. Criar estrutura de pastas e arquivos base (todos vazios/com esqueleto)
2. `requirements.txt` e `.env.example`
3. `config/tabelas.json`
4. `sql/003_create_log.sql` → tabela tb_log_etl
5. `sql/001_create_staging.sql` → tabela staging
6. `sql/002_create_producao.sql` → tabela tb_problemas_gov_ti
7. `sql/004_stored_procedures.sql` → SP de MERGE
8. `sql/005_views_kpi.sql` → 7 views KPI
9. `bot/logger.py`
10. `bot/database.py`
11. `bot/alertas.py`
12. `bot/etl.py`
13. `bot/watcher.py`
14. `scripts/reprocessar.py`
15. `README.md` com instruções de instalação e uso

---

## 📌 Observações Importantes

- A planilha atual tem **28 registros** e **16 colunas úteis**
- A coluna `dt_conclusao` tem dados inconsistentes (datas, textos como "Pendente - Triagem", "2º TRI") — tratar sempre como VARCHAR(100)
- As colunas de status histórico (`status_14_04`, `status_17_04`, `status_22_04`) tendem a crescer com novas datas a cada semana — o design do banco deve prever isso
- A aba do Excel se chama **Página4** — sempre ler esta aba especificamente
- As colunas com espaço em branco (" ", " .1" até " .10") são lixo da planilha e devem ser sempre removidas no ETL

---

## 🖥️ Deploy em VM — Passo a Passo

### Pré-requisitos na VM
- Linux (Ubuntu 22.04+ recomendado)
- Docker + Docker Compose v2
- Python 3.11+
- ODBC Driver 17 for SQL Server

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

> Se a VM tiver domínio público, substitua pelo Let's Encrypt:
> `sudo certbot certonly --standalone -d seu.dominio.com`
> e atualize os caminhos em `nginx/metabase.conf`.

### 5. Subir o Metabase + nginx via Docker Compose

```bash
# Definir MB_SITE_URL com HTTPS antes de subir
# No .env: MB_SITE_URL=https://ip-da-vm

docker compose up -d

# Verificar saúde
docker compose ps
docker compose logs -f metabase
docker compose logs -f nginx
```

Acesse em `https://ip-da-vm` para o setup inicial (aceitar o certificado auto-assinado no navegador).

### 5. Instalar o FlowETL como serviço systemd

```bash
sudo cp flowetl.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable flowetl
sudo systemctl start flowetl

# Verificar status e logs
sudo systemctl status flowetl
sudo journalctl -u flowetl -f
```

### 6. Compartilhar pasta_monitorada via Samba (opcional)

Instalar e configurar o Samba para que usuários Windows depositem `.xlsx` diretamente:

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

# Ver status do Metabase
docker compose ps
docker compose logs metabase --tail=50
```

### Notas de segurança para VM
- O `.env` deve ter permissão `600` (`chmod 600 .env`)
- O Metabase **não expõe porta diretamente** — todo acesso passa pelo nginx (:443)
- O serviço systemd roda como usuário `flowetl` sem privilégios de root (`NoNewPrivileges=true`)
- Metabase usa PostgreSQL dedicado (não H2) para garantir persistência e integridade dos dados
- Certificados SSL ficam em `nginx/certs/` — nunca versionar (estão no `.gitignore`)
- Para debug local sem nginx: descomentar `ports: ["3001:3000"]` no `docker-compose.yml`

### Arquivos de infraestrutura

| Arquivo | Função |
|---|---|
| `docker-compose.yml` | Orquestra nginx + Metabase + PostgreSQL |
| `flowetl.service` | Daemon systemd do watcher Python |
| `nginx/metabase.conf` | Proxy reverso HTTPS para o Metabase |
| `nginx/certs/` | Certificados SSL (gerados na VM, não versionados) |
