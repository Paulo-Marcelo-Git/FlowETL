-- =============================================================
-- Criação do banco de dados FlowETL
-- HML  →  HML-DBFLOWETL01
-- PRD  →  PRD-DBFLOWETL01
--
-- Execute este script conectado ao master como SA ou sysadmin.
-- Troque o nome conforme o ambiente antes de rodar.
-- =============================================================

USE master;
GO

-- Homologação
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'HML-DBFLOWETL01')
BEGIN
    CREATE DATABASE [HML-DBFLOWETL01]
        COLLATE Latin1_General_CI_AS;   -- insensível a maiúsculas/acentuação
    PRINT 'Banco HML-DBFLOWETL01 criado com sucesso.';
END
ELSE
BEGIN
    PRINT 'Banco HML-DBFLOWETL01 já existe — nenhuma ação necessária.';
END
GO

-- Produção (descomente quando for criar em PRD)
-- IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'PRD-DBFLOWETL01')
-- BEGIN
--     CREATE DATABASE [PRD-DBFLOWETL01]
--         COLLATE Latin1_General_CI_AS;
--     PRINT 'Banco PRD-DBFLOWETL01 criado com sucesso.';
-- END
-- GO

-- Confirma criação
SELECT name, collation_name, recovery_model_desc, state_desc
FROM sys.databases
WHERE name IN ('HML-DBFLOWETL01', 'PRD-DBFLOWETL01');
