-- =============================================================
-- Tabela de log de erros do Metabase
-- Alimentada por bot/monitor_metabase.py a cada 5 minutos
-- =============================================================

IF OBJECT_ID('dbo.tb_log_metabase', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.tb_log_metabase (
        id_log    INT IDENTITY(1,1) PRIMARY KEY,
        fonte     VARCHAR(30)  NOT NULL,       -- 'docker'
        nivel     VARCHAR(10)  NOT NULL,       -- 'ERROR' | 'WARN'
        modulo    VARCHAR(200) NULL,           -- ex: metabase.query-processor
        mensagem  VARCHAR(MAX) NOT NULL,
        dt_evento DATETIME     NULL,           -- timestamp da linha de log
        dt_insert DATETIME     NOT NULL DEFAULT GETDATE()
    );
    CREATE INDEX IX_log_metabase_dt ON dbo.tb_log_metabase (dt_evento);
    PRINT 'tb_log_metabase criada.';
END
ELSE
    PRINT 'tb_log_metabase já existe.';
