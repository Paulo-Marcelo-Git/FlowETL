# Design — Fila de Retry Automático (tb_retry_queue)

**Data:** 2026-05-25
**Área:** Resiliência / Confiabilidade
**Status:** Aprovado

---

## Problema

Arquivos que falham no ETL são movidos para `/erros/` e exigem intervenção manual via
`scripts/reprocessar.py`. Erros de infraestrutura (banco fora, timeout) e erros de dado
(coluna inválida, parse) caem no mesmo balde — sem distinção, sem retry automático, sem
visibilidade de quantas vezes já foi tentado.

---

## Solução

Fila de retry persistida no SQL Server com classificação automática de erros,
backoff exponencial por tipo, e notificações Telegram no ciclo completo.

---

## Arquitetura

```
xlsx detectado pelo watcher
        │
        ▼
  etl.processar_arquivo()
        │
   ┌────┴─────┐
   │ sucesso  │ falha
   │          │
   ▼          ▼
processados/ retry.classificar_erro(exc)
                     │
            ┌────────┴────────┐
            │ infra            │ dado
            │ (DB, rede,       │ (parse, coluna
            │  timeout)        │  inválida, etc.)
            ▼                  ▼
    enfileirar('infra')  enfileirar('dado')
    backoff paciente     backoff conservador
    (5 tentativas)       (2 tentativas + avisa)
            │                  │
            └────────┬─────────┘
                     ▼
             tb_retry_queue
                     │
              APScheduler job
              (a cada 1 min)
                     │
        dt_proxima_tentativa <= agora?
                     │ sim
                     ▼
           etl.processar_arquivo()
                     │
            ┌────────┴─────────┐
            │ sucesso          │ falha
            ▼                  ▼
   remove da fila        incrementa tentativa
   Telegram ✅           recalcula dt_proxima_tentativa
                                │
                         qt_tentativas >= max?
                                │ sim
                                ▼
                        ds_status = 'desistiu'
                        Telegram ❌ (resumo)
```

---

## Classificação de Erros

`retry.classificar_erro(exc)` retorna `'infra'` ou `'dado'`.

**Erros de infra** (retry paciente):
- `sqlalchemy.exc.OperationalError`
- `TimeoutError`, `ConnectionError`, `OSError`
- Mensagem contém: `"timeout"`, `"connection"`, `"unavailable"`

**Erros de dado** (retry rápido + desiste logo):
- Tudo que não for infra: `ValueError`, `KeyError`, `pandas` parse errors, etc.

### Tabela de backoff

| Tipo  | Tentativas máx | Intervalos entre tentativas        |
|-------|---------------|-------------------------------------|
| infra | 5             | 1 min → 5 min → 30 min → 2h → 8h   |
| dado  | 2             | 5 min → 60 min                      |

Erros de dado têm menos tentativas porque provavelmente precisam de correção manual
no Excel — o sistema avisa rápido e desiste.

---

## Schema — `tb_retry_queue`

```sql
CREATE TABLE dbo.tb_retry_queue (
    id_retry             INT IDENTITY PRIMARY KEY,
    nm_arquivo           VARCHAR(500)  NOT NULL,
    caminho_arquivo      VARCHAR(1000) NOT NULL,
    tipo_erro            VARCHAR(10)   NOT NULL,  -- 'infra' | 'dado'
    qt_tentativas        INT           NOT NULL DEFAULT 0,
    qt_max_tentativas    INT           NOT NULL,
    dt_primeira_falha    DATETIME      NOT NULL DEFAULT GETDATE(),
    dt_proxima_tentativa DATETIME      NOT NULL,
    dt_ultima_tentativa  DATETIME      NULL,
    ds_ultimo_erro       VARCHAR(MAX)  NULL,
    ds_status            VARCHAR(20)   NOT NULL DEFAULT 'aguardando',
    dt_insert            DATETIME      NOT NULL DEFAULT GETDATE()
);

-- Impede duplicatas enquanto o arquivo ainda está na fila
CREATE UNIQUE INDEX uq_retry_arquivo_ativo
    ON dbo.tb_retry_queue (nm_arquivo)
    WHERE ds_status IN ('aguardando', 'processando');
```

---

## Novos Artefatos

| Artefato | Descrição |
|----------|-----------|
| `sql/007_create_retry_queue.sql` | Cria `tb_retry_queue` e índice único |
| `bot/retry.py` | Classificação, enfileiramento, job APScheduler |

## Alterações em Arquivos Existentes

| Arquivo | Alteração |
|---------|-----------|
| `bot/etl.py` | Chama `retry.enfileirar()` no bloco `except` (2 linhas) |
| `bot/alertas.py` | Adiciona `alerta_retry_sucesso()` e `alerta_retry_desistiu()` + registra job no scheduler |

---

## Interface de `bot/retry.py`

```python
def classificar_erro(exc: Exception) -> Literal['infra', 'dado']:
    """Retorna o tipo de erro com base na classe e mensagem da exceção."""

def enfileirar(nm_arquivo: str, caminho: str, exc: Exception) -> None:
    """Insere ou ignora (idempotente) na tb_retry_queue."""

def processar_fila() -> None:
    """Job APScheduler — roda a cada 1 min. Processa entradas elegíveis.
    No início de cada ciclo, reseta entradas presas em 'processando' há
    mais de 30 min de volta para 'aguardando' (recovery após crash do watcher)."""

def _calcular_proxima_tentativa(tipo: str, tentativa: int) -> datetime:
    """Retorna datetime da próxima tentativa conforme tabela de backoff."""
```

---

## Alteração em `bot/etl.py`

Bloco `except` existente recebe duas linhas adicionais:

```python
# ANTES
alerta_falha_telegram(nm_arquivo, ds_erro)
return False

# DEPOIS
from bot.retry import enfileirar
enfileirar(nm_arquivo, str(caminho), exc)
alerta_falha_telegram(nm_arquivo, ds_erro)
return False
```

---

## Mensagens Telegram

**Retry bem-sucedido:**
```
🔁 FlowETL — Retry bem-sucedido
📄 Arquivo: gproblemas_abril_2024.xlsx
🔢 Tentativa: 3/5
⏱️ Horário: 2024-04-25 14:32:00
```

**Desistência:**
```
⛔ FlowETL — Desistindo do arquivo
📄 Arquivo: gproblemas_abril_2024.xlsx
🔴 Tipo: infra | Tentativas: 5/5
💬 Último erro: [mensagem]
⏱️ Horário: 2024-04-25 14:32:00
```

---

## Tratamento de Casos de Borda

| Cenário | Comportamento |
|---------|---------------|
| Job demora mais de 1 min | Status `'processando'` impede reentrada no mesmo ciclo |
| Arquivo sumiu de `/erros/` | `FileNotFoundError` → classifica como `'dado'`, incrementa tentativas, loga aviso |
| Banco fora durante o job | Exceção capturada, logada; job não propaga erro ao APScheduler |
| Watcher cai com arquivo em `'processando'` | No próximo ciclo, `processar_fila` reseta entradas `'processando'` com `dt_ultima_tentativa` > 30 min atrás de volta para `'aguardando'` |
| Reprocessamento manual bem-sucedido | Arquivo sai de `/erros/`; próximo ciclo detecta `FileNotFoundError` → marca `'desistiu'` com mensagem explicativa |

---

## Validação

1. Smoke test manual: inserir arquivo inválido → confirmar entrada na fila → confirmar retry nos logs → confirmar Telegram
2. Inspeção da `tb_retry_queue` via `sqlcmd` ou card no Metabase
3. Teste de backoff: parar container `sqlserver` → confirmar que intervalos respeitam a tabela de backoff

---

## O Que Não Muda

- `bot/watcher.py`
- `bot/database.py`
- `config/tabelas.json`
- `scripts/reprocessar.py` — continua funcionando para reprocessamento manual
