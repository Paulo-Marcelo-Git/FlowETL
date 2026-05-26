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
        WHERE ds_status IN ('aguardando', 'processando');
    PRINT 'Índice uq_retry_arquivo_ativo criado.';
END
ELSE
    PRINT 'Índice uq_retry_arquivo_ativo já existe — ignorado.';
GO
