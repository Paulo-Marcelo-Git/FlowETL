# FlowETL

Pipeline automatizado **Excel → SQL Server → Metabase**.

Monitora uma pasta local, detecta arquivos Excel automaticamente, processa e insere os dados no SQL Server, e os disponibiliza para dashboards de KPI no Metabase. Em caso de falha, dispara alertas via Telegram e e-mail.

---

## Requisitos

- Python 3.10+
- SQL Server com ODBC Driver 17 (ou superior)
- Conta no Telegram (Bot + Chat ID)
- Conta de e-mail com SMTP habilitado (ex.: Gmail com senha de app)

---

## Instalação

```bash
# 1. Clone o repositório
git clone <url-do-repositorio>
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
```

---

## Preparação do Banco de Dados

Execute os scripts SQL na seguinte ordem no SQL Server:

```sql
-- 1. Tabela de log (auditoria)
sql/003_create_log.sql

-- 2. Tabela de staging
sql/001_create_staging.sql

-- 3. Tabela de produção
sql/002_create_producao.sql

-- 4. Stored Procedure de MERGE
sql/004_stored_procedures.sql

-- 5. Views KPI para Metabase
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
    ├─ ✅ Sucesso → move para processados/YYYY-MM/
    └─ ❌ Falha   → move para erros/ + alerta Telegram + log
```

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
├── .env                # Credenciais (não versionar)
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
4. Registre o novo mapeamento em `bot/database.py` → `TABELA_CONFIG`
