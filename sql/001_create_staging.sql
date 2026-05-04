-- =============================================================
-- Tabela de staging: recebe os dados brutos do Excel
-- Truncada e recarregada a cada execução do ETL
-- =============================================================

IF NOT EXISTS (
    SELECT 1 FROM sys.tables WHERE name = 'stg_problemas_gov_ti'
)
BEGIN
    CREATE TABLE dbo.stg_problemas_gov_ti (
        numero              INT             NULL,
        prioridade          VARCHAR(5)      NULL,
        titulo              VARCHAR(500)    NULL,
        descricao           VARCHAR(MAX)    NULL,
        dt_abertura         DATE            NULL,
        dt_conclusao        VARCHAR(100)    NULL,  -- Inconsistente na planilha — sempre VARCHAR
        status              VARCHAR(100)    NULL,
        gerente_responsavel VARCHAR(200)    NULL,
        departamento_relator VARCHAR(200)   NULL,
        jornada_impactada   VARCHAR(200)    NULL,
        sistema             VARCHAR(200)    NULL,
        paliativo           VARCHAR(10)     NULL,
        impacto             VARCHAR(MAX)    NULL,
        status_14_04        VARCHAR(MAX)    NULL,
        status_17_04        VARCHAR(MAX)    NULL,
        status_22_04        VARCHAR(MAX)    NULL,
        status_27_04        VARCHAR(MAX)    NULL,
        nm_arquivo_origem   VARCHAR(500)    NULL
    );

    PRINT 'Tabela stg_problemas_gov_ti criada com sucesso.';
END
ELSE
BEGIN
    PRINT 'Tabela stg_problemas_gov_ti já existe — nenhuma ação necessária.';
END
