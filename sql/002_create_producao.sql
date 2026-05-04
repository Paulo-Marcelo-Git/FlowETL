-- =============================================================
-- Tabela de produção: dados consolidados com controle de auditoria
-- Chave de negócio: numero (INT)
-- =============================================================

IF NOT EXISTS (
    SELECT 1 FROM sys.tables WHERE name = 'tb_problemas_gov_ti'
)
BEGIN
    CREATE TABLE dbo.tb_problemas_gov_ti (
        numero              INT             NOT NULL,
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
        nm_arquivo_origem   VARCHAR(500)    NULL,
        dt_insert           DATETIME        NOT NULL DEFAULT GETDATE(),
        dt_atualizacao      DATETIME        NULL,

        CONSTRAINT PK_tb_problemas_gov_ti PRIMARY KEY CLUSTERED (numero)
    );

    -- Índices para as colunas mais consultadas nos KPIs
    CREATE NONCLUSTERED INDEX IX_problemas_status
        ON dbo.tb_problemas_gov_ti (status);

    CREATE NONCLUSTERED INDEX IX_problemas_prioridade
        ON dbo.tb_problemas_gov_ti (prioridade);

    CREATE NONCLUSTERED INDEX IX_problemas_gerente
        ON dbo.tb_problemas_gov_ti (gerente_responsavel);

    PRINT 'Tabela tb_problemas_gov_ti criada com sucesso.';
END
ELSE
BEGIN
    PRINT 'Tabela tb_problemas_gov_ti já existe — nenhuma ação necessária.';
END
