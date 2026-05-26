-- sql/008_create_schema_registry.sql
USE [HML-DBFLOWETL01];
GO

IF OBJECT_ID('dbo.tb_schema_registry', 'U') IS NULL
CREATE TABLE dbo.tb_schema_registry (
    id_schema       INT IDENTITY(1,1) NOT NULL,
    nm_tabela       VARCHAR(200)      NOT NULL,
    nm_staging      VARCHAR(200)      NOT NULL,
    nm_sp_merge     VARCHAR(200)      NOT NULL,
    nm_chave        VARCHAR(100)      NOT NULL,
    colunas_base    VARCHAR(MAX)      NOT NULL,
    id_dashboard_mb INT               NULL,
    nm_dashboard_mb VARCHAR(200)      NULL,
    dt_criacao      DATETIME          NOT NULL DEFAULT GETDATE(),
    dt_atualizacao  DATETIME          NULL,
    CONSTRAINT PK_tb_schema_registry  PRIMARY KEY (id_schema),
    CONSTRAINT UQ_schema_registry_tab UNIQUE (nm_tabela)
);
GO
