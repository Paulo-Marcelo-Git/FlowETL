-- =============================================================
-- Tabela de auditoria de execuções do ETL
-- Criada antes das demais para ser referenciada nos SPs
-- =============================================================

IF NOT EXISTS (
    SELECT 1 FROM sys.tables WHERE name = 'tb_log_etl'
)
BEGIN
    CREATE TABLE dbo.tb_log_etl (
        id_log              INT IDENTITY(1,1)   NOT NULL,
        nm_arquivo          VARCHAR(500)        NOT NULL,
        nm_tabela_destino   VARCHAR(200)        NOT NULL,
        dt_processamento    DATETIME            NOT NULL DEFAULT GETDATE(),
        qt_linhas_recebidas INT                 NULL,
        qt_linhas_inseridas INT                 NULL,
        qt_linhas_rejeitadas INT                NULL,
        ds_status           VARCHAR(20)         NOT NULL, -- 'sucesso' | 'falha' | 'parcial'
        ds_erro             VARCHAR(MAX)        NULL,
        tm_duracao_seg      DECIMAL(10, 2)      NULL,

        CONSTRAINT PK_tb_log_etl PRIMARY KEY CLUSTERED (id_log)
    );

    PRINT 'Tabela tb_log_etl criada com sucesso.';
END
ELSE
BEGIN
    PRINT 'Tabela tb_log_etl já existe — nenhuma ação necessária.';
END
