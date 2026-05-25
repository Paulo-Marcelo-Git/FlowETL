# Retry Queue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implementar fila de retry automático persistida em `tb_retry_queue`, com classificação de erros em 'infra'/'dado', backoff exponencial e alertas Telegram no ciclo completo.

**Architecture:** Um novo módulo `bot/retry.py` cuida de classificar erros, enfileirar no banco e processar a fila a cada 1 minuto via APScheduler. O `bot/etl.py` chama `enfileirar()` no bloco `except` existente (2 linhas adicionais). O `bot/alertas.py` recebe 2 novas funções de alerta e registra o job no scheduler.

**Tech Stack:** Python 3.11+, SQLAlchemy, APScheduler, SQL Server 2022, pytest

---

## Mapa de Arquivos

| Arquivo | Ação | Responsabilidade |
|---------|------|-----------------|
| `sql/007_create_retry_queue.sql` | Criar | DDL da tabela e índice |
| `bot/retry.py` | Criar | Classificação, backoff, enfileiramento, job |
| `bot/alertas.py` | Modificar | +2 funções de alerta, +1 job no scheduler |
| `bot/etl.py` | Modificar | Capturar path em /erros/ + chamar enfileirar() |
| `tests/test_retry.py` | Criar | Testes das funções puras (sem DB) |

---

## Task 1: SQL Migration — tb_retry_queue

**Files:**
- Create: `sql/007_create_retry_queue.sql`

- [ ] **Step 1: Criar o arquivo SQL**

```sql
-- sql/007_create_retry_queue.sql
-- Executar conectado ao banco correto: HML-DBFLOWETL01 ou PRD-DBFLOWETL01

IF OBJECT_ID('dbo.tb_retry_queue', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.tb_retry_queue (
        id_retry             INT IDENTITY(1,1) PRIMARY KEY,
        nm_arquivo           VARCHAR(500)  NOT NULL,
        caminho_arquivo      VARCHAR(1000) NOT NULL,
        tipo_erro            VARCHAR(10)   NOT NULL,
        qt_tentativas        INT           NOT NULL DEFAULT 0,
        qt_max_tentativas    INT           NOT NULL,
        dt_primeira_falha    DATETIME      NOT NULL DEFAULT GETDATE(),
        dt_proxima_tentativa DATETIME      NOT NULL,
        dt_ultima_tentativa  DATETIME      NULL,
        ds_ultimo_erro       VARCHAR(MAX)  NULL,
        ds_status            VARCHAR(20)   NOT NULL DEFAULT 'aguardando',
        dt_insert            DATETIME      NOT NULL DEFAULT GETDATE()
    );
    PRINT 'Tabela tb_retry_queue criada.';
END
ELSE
    PRINT 'Tabela tb_retry_queue já existe — ignorado.';
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'uq_retry_arquivo_ativo'
      AND object_id = OBJECT_ID('dbo.tb_retry_queue')
)
BEGIN
    CREATE UNIQUE INDEX uq_retry_arquivo_ativo
        ON dbo.tb_retry_queue (nm_arquivo)
        WHERE ds_status = 'aguardando' OR ds_status = 'processando';
    PRINT 'Índice uq_retry_arquivo_ativo criado.';
END
ELSE
    PRINT 'Índice uq_retry_arquivo_ativo já existe — ignorado.';
GO
```

- [ ] **Step 2: Aplicar migration no container SQL Server**

```bash
# Substituir HML-DBFLOWETL01 por PRD-DBFLOWETL01 se estiver em produção
docker exec -i flowetl-sqlserver \
  /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$MSSQL_SA_PASSWORD" \
  -d "HML-DBFLOWETL01" -No \
  -i /dev/stdin < sql/007_create_retry_queue.sql
```

Saída esperada:
```
Tabela tb_retry_queue criada.
Índice uq_retry_arquivo_ativo criado.
```

- [ ] **Step 3: Verificar criação**

```bash
docker exec -i flowetl-sqlserver \
  /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$MSSQL_SA_PASSWORD" \
  -d "HML-DBFLOWETL01" -No \
  -Q "SELECT name FROM sys.tables WHERE name = 'tb_retry_queue'"
```

Saída esperada: `tb_retry_queue`

- [ ] **Step 4: Commit**

```bash
git add sql/007_create_retry_queue.sql
git commit -m "feat: cria tabela tb_retry_queue para fila de retry automático"
```

---

## Task 2: bot/retry.py — Funções puras + testes

**Files:**
- Create: `bot/retry.py` (funções `classificar_erro` e `_calcular_proxima_tentativa`)
- Create: `tests/test_retry.py`

- [ ] **Step 1: Criar tests/test_retry.py com testes que DEVEM FALHAR**

```bash
mkdir -p tests
touch tests/__init__.py
```

```python
# tests/test_retry.py
from datetime import datetime, timedelta
import pytest
from sqlalchemy.exc import OperationalError
from bot.retry import classificar_erro, _calcular_proxima_tentativa


class TestClassificarErro:
    def test_operational_error_e_infra(self):
        exc = OperationalError("connection refused", None, None)
        assert classificar_erro(exc) == 'infra'

    def test_timeout_error_e_infra(self):
        assert classificar_erro(TimeoutError()) == 'infra'

    def test_connection_error_e_infra(self):
        assert classificar_erro(ConnectionError()) == 'infra'

    def test_os_error_e_infra(self):
        assert classificar_erro(OSError()) == 'infra'

    def test_file_not_found_e_dado(self):
        # FileNotFoundError é subclasse de OSError — deve ser 'dado'
        assert classificar_erro(FileNotFoundError()) == 'dado'

    def test_value_error_e_dado(self):
        assert classificar_erro(ValueError('coluna não encontrada')) == 'dado'

    def test_key_error_e_dado(self):
        assert classificar_erro(KeyError('numero')) == 'dado'

    def test_keyword_timeout_na_mensagem_e_infra(self):
        assert classificar_erro(Exception('TCP timeout reached')) == 'infra'

    def test_keyword_connection_na_mensagem_e_infra(self):
        assert classificar_erro(Exception('connection reset by peer')) == 'infra'

    def test_keyword_unavailable_na_mensagem_e_infra(self):
        assert classificar_erro(Exception('server unavailable')) == 'infra'

    def test_erro_generico_e_dado(self):
        assert classificar_erro(Exception('coluna x nao existe')) == 'dado'


class TestCalcularProximaTentativa:
    def _delta(self, resultado: datetime) -> timedelta:
        return resultado - datetime.now()

    def test_infra_tentativa_0_e_1_minuto(self):
        delta = self._delta(_calcular_proxima_tentativa('infra', 0))
        assert timedelta(seconds=55) < delta < timedelta(seconds=65)

    def test_infra_tentativa_1_e_5_minutos(self):
        delta = self._delta(_calcular_proxima_tentativa('infra', 1))
        assert timedelta(minutes=4, seconds=55) < delta < timedelta(minutes=5, seconds=5)

    def test_infra_tentativa_2_e_30_minutos(self):
        delta = self._delta(_calcular_proxima_tentativa('infra', 2))
        assert timedelta(minutes=29, seconds=55) < delta < timedelta(minutes=30, seconds=5)

    def test_infra_tentativa_3_e_2_horas(self):
        delta = self._delta(_calcular_proxima_tentativa('infra', 3))
        assert timedelta(hours=1, minutes=59, seconds=55) < delta < timedelta(hours=2, seconds=5)

    def test_infra_tentativa_4_e_8_horas(self):
        delta = self._delta(_calcular_proxima_tentativa('infra', 4))
        assert timedelta(hours=7, minutes=59, seconds=55) < delta < timedelta(hours=8, seconds=5)

    def test_indice_alem_do_limite_usa_ultimo_backoff(self):
        # tentativa=99 → usa backoff[-1] = 480 min (8h)
        delta = self._delta(_calcular_proxima_tentativa('infra', 99))
        assert timedelta(hours=7, minutes=59, seconds=55) < delta < timedelta(hours=8, seconds=5)

    def test_dado_tentativa_0_e_5_minutos(self):
        delta = self._delta(_calcular_proxima_tentativa('dado', 0))
        assert timedelta(minutes=4, seconds=55) < delta < timedelta(minutes=5, seconds=5)

    def test_dado_tentativa_1_e_60_minutos(self):
        delta = self._delta(_calcular_proxima_tentativa('dado', 1))
        assert timedelta(minutes=59, seconds=55) < delta < timedelta(minutes=60, seconds=5)
```

- [ ] **Step 2: Instalar pytest e rodar — confirmar FALHA por ImportError**

```bash
pip install pytest
pytest tests/test_retry.py -v 2>&1 | head -20
```

Saída esperada: `ModuleNotFoundError: No module named 'bot.retry'`

- [ ] **Step 3: Criar bot/retry.py com as funções puras**

```python
# bot/retry.py
"""
Fila de retry automático para arquivos que falharam no ETL.
Classifica erros em 'infra' ou 'dado', persiste estado em tb_retry_queue
e reprocessa automaticamente com backoff exponencial.
"""

import os
from datetime import datetime, timedelta
from typing import Literal

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from bot.database import obter_engine
from bot.logger import configurar_logger

logger = configurar_logger(__name__)

# Intervalos de backoff em minutos, indexados pelo número da tentativa (0-based)
_BACKOFF: dict = {
    'infra': [1, 5, 30, 120, 480],
    'dado':  [5, 60],
}

_MAX_TENTATIVAS: dict = {
    'infra': 5,
    'dado':  2,
}

_ERROS_INFRA = (OperationalError, TimeoutError, ConnectionError, OSError)
_KEYWORDS_INFRA = ('timeout', 'connection', 'unavailable')


def classificar_erro(exc: Exception) -> Literal['infra', 'dado']:
    """Retorna 'infra' para erros transitórios de rede/banco, 'dado' para o resto."""
    if isinstance(exc, FileNotFoundError):
        # FileNotFoundError é subclasse de OSError — deve ser tratado como dado
        return 'dado'
    if isinstance(exc, _ERROS_INFRA):
        return 'infra'
    msg = str(exc).lower()
    if any(k in msg for k in _KEYWORDS_INFRA):
        return 'infra'
    return 'dado'


def _calcular_proxima_tentativa(tipo: str, tentativa: int) -> datetime:
    """
    Retorna o datetime da próxima tentativa.
    tentativa = índice 0-based da tentativa que acabou de falhar
    (0 = ainda não houve retry, calcula espera antes do primeiro).
    """
    intervalos = _BACKOFF.get(tipo, _BACKOFF['dado'])
    idx = min(tentativa, len(intervalos) - 1)
    return datetime.now() + timedelta(minutes=intervalos[idx])
```

- [ ] **Step 4: Rodar testes — devem passar**

```bash
pytest tests/test_retry.py -v
```

Saída esperada:
```
tests/test_retry.py::TestClassificarErro::test_operational_error_e_infra PASSED
tests/test_retry.py::TestClassificarErro::test_timeout_error_e_infra PASSED
tests/test_retry.py::TestClassificarErro::test_connection_error_e_infra PASSED
tests/test_retry.py::TestClassificarErro::test_os_error_e_infra PASSED
tests/test_retry.py::TestClassificarErro::test_file_not_found_e_dado PASSED
tests/test_retry.py::TestClassificarErro::test_value_error_e_dado PASSED
tests/test_retry.py::TestClassificarErro::test_key_error_e_dado PASSED
tests/test_retry.py::TestClassificarErro::test_keyword_timeout_na_mensagem_e_infra PASSED
tests/test_retry.py::TestClassificarErro::test_keyword_connection_na_mensagem_e_infra PASSED
tests/test_retry.py::TestClassificarErro::test_keyword_unavailable_na_mensagem_e_infra PASSED
tests/test_retry.py::TestClassificarErro::test_erro_generico_e_dado PASSED
tests/test_retry.py::TestCalcularProximaTentativa::test_infra_tentativa_0_e_1_minuto PASSED
... (todos os 19 testes passando)
19 passed
```

- [ ] **Step 5: Commit**

```bash
git add bot/retry.py tests/__init__.py tests/test_retry.py
git commit -m "feat: adiciona bot/retry.py com classificar_erro e backoff (TDD)"
```

---

## Task 3: bot/retry.py — enfileirar()

**Files:**
- Modify: `bot/retry.py` (adicionar função `enfileirar`)

- [ ] **Step 1: Adicionar enfileirar() ao final de bot/retry.py**

Adicionar após `_calcular_proxima_tentativa`:

```python
def enfileirar(nm_arquivo: str, caminho: str, exc: Exception) -> None:
    """Insere arquivo na fila de retry. Idempotente: ignora se já está ativo na fila."""
    tipo = classificar_erro(exc)
    ds_erro = str(exc)[:4000]
    proxima = _calcular_proxima_tentativa(tipo, 0)
    max_tent = _MAX_TENTATIVAS[tipo]

    try:
        engine = obter_engine()
        with engine.begin() as conn:
            existe = conn.execute(
                text("""
                    SELECT COUNT(*) FROM dbo.tb_retry_queue
                    WHERE nm_arquivo = :nm
                      AND ds_status IN ('aguardando', 'processando')
                """),
                {'nm': nm_arquivo},
            ).scalar()
            if existe:
                logger.info(f'{nm_arquivo} já está na fila de retry — ignorado.')
                return
            conn.execute(
                text("""
                    INSERT INTO dbo.tb_retry_queue
                        (nm_arquivo, caminho_arquivo, tipo_erro, qt_tentativas,
                         qt_max_tentativas, dt_proxima_tentativa, ds_ultimo_erro, ds_status)
                    VALUES
                        (:nm, :caminho, :tipo, 0, :max_t, :proxima, :erro, 'aguardando')
                """),
                {
                    'nm': nm_arquivo, 'caminho': caminho, 'tipo': tipo,
                    'max_t': max_tent, 'proxima': proxima, 'erro': ds_erro,
                },
            )
        logger.info(f'{nm_arquivo} enfileirado para retry ({tipo}, próxima: {proxima}).')
    except Exception as db_exc:
        logger.error(f'Falha ao enfileirar {nm_arquivo}: {db_exc}')
```

- [ ] **Step 2: Verificar que os testes existentes ainda passam**

```bash
pytest tests/test_retry.py -v
```

Saída esperada: todos os 19 testes passando.

- [ ] **Step 3: Commit**

```bash
git add bot/retry.py
git commit -m "feat: adiciona enfileirar() em bot/retry.py"
```

---

## Task 4: bot/alertas.py — Funções de alerta de retry

**Files:**
- Modify: `bot/alertas.py` (adicionar `alerta_retry_sucesso` e `alerta_retry_desistiu`)

- [ ] **Step 1: Adicionar as duas funções após alerta_sucesso_telegram (linha ~72)**

Localizar o bloco:
```python
def alerta_sucesso_telegram(nm_arquivo: str, qt_linhas: int) -> None:
    ...
    enviar_telegram(mensagem)


# -------------------------------------------------------------------- Email
```

Inserir entre `alerta_sucesso_telegram` e o bloco `# Email`:

```python
def alerta_retry_sucesso(nm_arquivo: str, tentativa: int, max_tentativas: int) -> None:
    """Alerta Telegram quando um retry automático tem sucesso."""
    agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    mensagem = (
        '<b>🔁 FlowETL — Retry bem-sucedido</b>\n'
        f'📄 Arquivo: <code>{nm_arquivo}</code>\n'
        f'🔢 Tentativa: {tentativa}/{max_tentativas}\n'
        f'⏱️ Horário: {agora}'
    )
    enviar_telegram(mensagem)


def alerta_retry_desistiu(
    nm_arquivo: str,
    tipo_erro: str,
    tentativas: int,
    max_tentativas: int,
    ultimo_erro: str,
) -> None:
    """Alerta Telegram quando o sistema desiste de reprocessar um arquivo."""
    agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    mensagem = (
        '<b>⛔ FlowETL — Desistindo do arquivo</b>\n'
        f'📄 Arquivo: <code>{nm_arquivo}</code>\n'
        f'🔴 Tipo: {tipo_erro} | Tentativas: {tentativas}/{max_tentativas}\n'
        f'💬 Último erro: {ultimo_erro[:200]}\n'
        f'⏱️ Horário: {agora}'
    )
    enviar_telegram(mensagem)
```

- [ ] **Step 2: Verificar sintaxe**

```bash
python -c "from bot.alertas import alerta_retry_sucesso, alerta_retry_desistiu; print('OK')"
```

Saída esperada: `OK`

- [ ] **Step 3: Commit**

```bash
git add bot/alertas.py
git commit -m "feat: adiciona alerta_retry_sucesso e alerta_retry_desistiu em alertas.py"
```

---

## Task 5: bot/retry.py — processar_fila() e _processar_entrada()

**Files:**
- Modify: `bot/retry.py` (adicionar as duas funções restantes)

- [ ] **Step 1: Adicionar _processar_entrada() e processar_fila() ao final de bot/retry.py**

```python
def processar_fila() -> None:
    """Job APScheduler executado a cada 1 min.

    Fluxo por ciclo:
    1. Reseta entradas presas em 'processando' há mais de 30 min (recovery após crash).
    2. Busca entradas elegíveis (aguardando + dt_proxima_tentativa <= agora).
    3. Para cada entrada, chama _processar_entrada().
    """
    try:
        engine = obter_engine()
    except Exception as exc:
        logger.error(f'processar_fila: sem conexão ao banco: {exc}')
        return

    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE dbo.tb_retry_queue
                SET ds_status = 'aguardando'
                WHERE ds_status = 'processando'
                  AND dt_ultima_tentativa < DATEADD(MINUTE, -30, GETDATE())
            """))

        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id_retry, nm_arquivo, caminho_arquivo, tipo_erro,
                       qt_tentativas, qt_max_tentativas
                FROM dbo.tb_retry_queue
                WHERE ds_status = 'aguardando'
                  AND dt_proxima_tentativa <= GETDATE()
            """)).mappings().all()

        for row in rows:
            _processar_entrada(engine, dict(row))

    except Exception as exc:
        logger.error(f'processar_fila: erro inesperado: {exc}')


def _processar_entrada(engine, row: dict) -> None:
    """Executa uma tentativa de retry para uma entrada da fila."""
    from bot.etl import processar_arquivo
    from bot.alertas import alerta_retry_sucesso, alerta_retry_desistiu

    id_retry = row['id_retry']
    nm_arquivo = row['nm_arquivo']
    caminho = row['caminho_arquivo']
    tipo = row['tipo_erro']
    qt = row['qt_tentativas']
    max_t = row['qt_max_tentativas']
    nova_tentativa = qt + 1

    # Arquivo sumiu de /erros/ — desiste imediatamente sem consumir tentativas
    if not os.path.exists(caminho):
        msg = 'Arquivo não encontrado em /erros/ (removido ou reprocessado manualmente)'
        logger.warning(f'Retry: {nm_arquivo} — {msg}')
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE dbo.tb_retry_queue
                SET ds_status = 'desistiu', qt_tentativas = :qt,
                    ds_ultimo_erro = :msg, dt_ultima_tentativa = GETDATE()
                WHERE id_retry = :id
            """), {'qt': nova_tentativa, 'msg': msg, 'id': id_retry})
        alerta_retry_desistiu(nm_arquivo, tipo, nova_tentativa, max_t, msg)
        return

    # Marcar como processando para evitar reentrada no mesmo ciclo
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE dbo.tb_retry_queue
            SET ds_status = 'processando', dt_ultima_tentativa = GETDATE()
            WHERE id_retry = :id
        """), {'id': id_retry})

    try:
        sucesso = processar_arquivo(caminho)
        ds_erro = None if sucesso else 'processar_arquivo retornou False sem exceção'
    except Exception as exc:
        sucesso = False
        ds_erro = str(exc)[:4000]

    if sucesso:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM dbo.tb_retry_queue WHERE id_retry = :id"),
                {'id': id_retry},
            )
        alerta_retry_sucesso(nm_arquivo, nova_tentativa, max_t)
        logger.info(f'Retry sucesso: {nm_arquivo} (tentativa {nova_tentativa}/{max_t})')

    elif nova_tentativa >= max_t:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE dbo.tb_retry_queue
                SET ds_status = 'desistiu', qt_tentativas = :qt,
                    ds_ultimo_erro = :erro, dt_ultima_tentativa = GETDATE()
                WHERE id_retry = :id
            """), {'qt': nova_tentativa, 'erro': ds_erro, 'id': id_retry})
        alerta_retry_desistiu(nm_arquivo, tipo, nova_tentativa, max_t, ds_erro or '')
        logger.warning(f'Retry desistiu: {nm_arquivo} após {nova_tentativa} tentativas.')

    else:
        proxima = _calcular_proxima_tentativa(tipo, nova_tentativa)
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE dbo.tb_retry_queue
                SET ds_status = 'aguardando', qt_tentativas = :qt,
                    ds_ultimo_erro = :erro, dt_proxima_tentativa = :proxima,
                    dt_ultima_tentativa = GETDATE()
                WHERE id_retry = :id
            """), {'qt': nova_tentativa, 'erro': ds_erro, 'proxima': proxima, 'id': id_retry})
        logger.info(
            f'Retry falhou: {nm_arquivo} tentativa {nova_tentativa}/{max_t}, '
            f'próxima: {proxima}'
        )
```

- [ ] **Step 2: Verificar sintaxe e imports**

```bash
python -c "from bot.retry import processar_fila, enfileirar, classificar_erro; print('OK')"
```

Saída esperada: `OK`

- [ ] **Step 3: Rodar testes**

```bash
pytest tests/test_retry.py -v
```

Saída esperada: todos os 19 testes passando.

- [ ] **Step 4: Commit**

```bash
git add bot/retry.py
git commit -m "feat: adiciona processar_fila e _processar_entrada em bot/retry.py"
```

---

## Task 6: bot/alertas.py — Registrar job no APScheduler

**Files:**
- Modify: `bot/alertas.py` (adicionar job `retry_queue` em `iniciar_scheduler_relatorio`)

- [ ] **Step 1: Adicionar import local e job dentro de iniciar_scheduler_relatorio()**

Localizar o bloco dentro de `iniciar_scheduler_relatorio` (após o import de `monitor_metabase`):

```python
    from bot.monitor_metabase import (
        verificar_e_alertar as _monitorar_metabase,
        reescanear_campos_metabase as _reescanear_campos,
    )
```

Adicionar a linha de import após esse bloco:

```python
    from bot.retry import processar_fila as _processar_fila_retry
```

Localizar o trecho com o último `add_job` existente:

```python
    _scheduler.add_job(
        func=_reescanear_campos,
        trigger='interval',
        minutes=5,
        id='rescan_campos_metabase',
        replace_existing=True,
    )
    _scheduler.start()
```

Inserir o novo job entre `rescan_campos_metabase` e `_scheduler.start()`:

```python
    _scheduler.add_job(
        func=_processar_fila_retry,
        trigger='interval',
        minutes=1,
        id='retry_queue',
        replace_existing=True,
    )
```

O trecho final de `iniciar_scheduler_relatorio` deve ficar assim:

```python
    _scheduler.add_job(
        func=_reescanear_campos,
        trigger='interval',
        minutes=5,
        id='rescan_campos_metabase',
        replace_existing=True,
    )
    _scheduler.add_job(
        func=_processar_fila_retry,
        trigger='interval',
        minutes=1,
        id='retry_queue',
        replace_existing=True,
    )
    _scheduler.start()
    logger.info(
        'Scheduler iniciado: relatório 08:00 BRT | monitor + rescan Metabase a cada 5 min '
        '| retry_queue a cada 1 min.'
    )
```

- [ ] **Step 2: Verificar sintaxe**

```bash
python -c "from bot.alertas import iniciar_scheduler_relatorio; print('OK')"
```

Saída esperada: `OK`

- [ ] **Step 3: Commit**

```bash
git add bot/alertas.py
git commit -m "feat: registra job retry_queue no APScheduler (1 min)"
```

---

## Task 7: bot/etl.py — Wirear enfileirar() no bloco except

**Files:**
- Modify: `bot/etl.py` (capturar path em /erros/ + chamar enfileirar)

- [ ] **Step 1: Modificar o bloco except em processar_arquivo**

Localizar o bloco `except` (por volta da linha 188):

```python
    except Exception as exc:
        ds_erro = str(exc)
        logger.error(f'Erro ao processar {nm_arquivo}: {ds_erro}', exc_info=True)

        # 12. Mover para /erros/ e disparar alertas
        try:
            _mover_arquivo(caminho, ERROS_DIR)
        except Exception as move_exc:
            logger.error(f'Falha ao mover arquivo para erros/: {move_exc}')

        try:
            engine = obter_engine()
            registrar_log_banco(
                engine=engine,
                nm_arquivo=nm_arquivo,
                nm_tabela_destino=nm_tabela,
                qt_linhas_recebidas=qt_recebidas,
                qt_linhas_inseridas=0,
                qt_linhas_rejeitadas=qt_recebidas,
                ds_status='falha',
                ds_erro=ds_erro,
                tm_duracao_seg=time.time() - inicio,
            )
        except Exception as log_exc:
            logger.error(f'Falha ao registrar log de erro no banco: {log_exc}')

        alerta_falha_telegram(nm_arquivo, ds_erro)
        return False
```

Substituir por:

```python
    except Exception as exc:
        ds_erro = str(exc)
        logger.error(f'Erro ao processar {nm_arquivo}: {ds_erro}', exc_info=True)

        # 12. Mover para /erros/ e disparar alertas
        caminho_em_erros = ERROS_DIR / nm_arquivo  # fallback se o move falhar
        try:
            caminho_em_erros = _mover_arquivo(caminho, ERROS_DIR)
        except Exception as move_exc:
            logger.error(f'Falha ao mover arquivo para erros/: {move_exc}')

        try:
            engine = obter_engine()
            registrar_log_banco(
                engine=engine,
                nm_arquivo=nm_arquivo,
                nm_tabela_destino=nm_tabela,
                qt_linhas_recebidas=qt_recebidas,
                qt_linhas_inseridas=0,
                qt_linhas_rejeitadas=qt_recebidas,
                ds_status='falha',
                ds_erro=ds_erro,
                tm_duracao_seg=time.time() - inicio,
            )
        except Exception as log_exc:
            logger.error(f'Falha ao registrar log de erro no banco: {log_exc}')

        from bot.retry import enfileirar
        enfileirar(nm_arquivo, str(caminho_em_erros), exc)
        alerta_falha_telegram(nm_arquivo, ds_erro)
        return False
```

- [ ] **Step 2: Verificar sintaxe**

```bash
python -c "from bot.etl import processar_arquivo; print('OK')"
```

Saída esperada: `OK`

- [ ] **Step 3: Rodar testes**

```bash
pytest tests/test_retry.py -v
```

Saída esperada: todos os 19 testes passando.

- [ ] **Step 4: Commit**

```bash
git add bot/etl.py
git commit -m "feat: wireia enfileirar() no bloco except de etl.processar_arquivo"
```

---

## Task 8: Smoke Test

- [ ] **Step 1: Confirmar que o banco tem a tabela vazia**

```bash
docker exec -i flowetl-sqlserver \
  /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$MSSQL_SA_PASSWORD" \
  -d "HML-DBFLOWETL01" -No \
  -Q "SELECT COUNT(*) AS total FROM dbo.tb_retry_queue"
```

Saída esperada: `total = 0`

- [ ] **Step 2: Verificar que o watcher inicia sem erros**

```bash
./scripts/watcher.sh restart
sleep 3
./scripts/watcher.sh status
```

Saída esperada: watcher rodando (PID listado).

- [ ] **Step 3: Depositar um arquivo com nome inválido para forçar falha**

```bash
# Criar arquivo .xlsx vazio com prefixo inválido para forçar erro
cp /dev/null pasta_monitorada/gproblemas_smoke_test_invalido.xlsx
sleep 5
```

Verificar no log:
```bash
./scripts/watcher.sh logs | tail -20
```

Saída esperada: linhas com `Erro ao processar` e `enfileirado para retry`.

- [ ] **Step 4: Confirmar entrada na fila**

```bash
docker exec -i flowetl-sqlserver \
  /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$MSSQL_SA_PASSWORD" \
  -d "HML-DBFLOWETL01" -No \
  -Q "SELECT nm_arquivo, tipo_erro, qt_tentativas, ds_status, dt_proxima_tentativa FROM dbo.tb_retry_queue"
```

Saída esperada: 1 linha com `nm_arquivo=gproblemas_smoke_test_invalido.xlsx`, `ds_status=aguardando`.

- [ ] **Step 5: Aguardar próxima tentativa e verificar incremento**

Aguardar o intervalo mínimo (dado: 5 min, ou forçar manualmente setando `dt_proxima_tentativa` para agora):

```bash
docker exec -i flowetl-sqlserver \
  /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$MSSQL_SA_PASSWORD" \
  -d "HML-DBFLOWETL01" -No \
  -Q "UPDATE dbo.tb_retry_queue SET dt_proxima_tentativa = GETDATE() WHERE ds_status = 'aguardando'"
```

Aguardar 1 min (próximo ciclo do job) e verificar:

```bash
docker exec -i flowetl-sqlserver \
  /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$MSSQL_SA_PASSWORD" \
  -d "HML-DBFLOWETL01" -No \
  -Q "SELECT nm_arquivo, tipo_erro, qt_tentativas, ds_status FROM dbo.tb_retry_queue"
```

Saída esperada: `qt_tentativas` incrementado (1 ou 2, dependendo do timing).

- [ ] **Step 6: Limpar arquivo de smoke test**

```bash
rm -f erros/gproblemas_smoke_test_invalido.xlsx
```

Aguardar o próximo ciclo do job: a entrada deve ser marcada como `'desistiu'` com a mensagem "Arquivo não encontrado em /erros/".

- [ ] **Step 7: Verificar estado final**

```bash
docker exec -i flowetl-sqlserver \
  /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$MSSQL_SA_PASSWORD" \
  -d "HML-DBFLOWETL01" -No \
  -Q "SELECT nm_arquivo, ds_status, ds_ultimo_erro FROM dbo.tb_retry_queue"
```

Saída esperada: `ds_status=desistiu`, `ds_ultimo_erro` contendo 'não encontrado'.

- [ ] **Step 8: Commit final**

```bash
git add .
git commit -m "test: smoke test da fila de retry concluído com sucesso"
```
