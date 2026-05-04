-- =============================================================
-- Views KPI para consumo no Metabase
-- =============================================================

-- 1. Problemas agrupados por status
IF OBJECT_ID('dbo.vw_problemas_por_status', 'V') IS NOT NULL
    DROP VIEW dbo.vw_problemas_por_status;
GO

CREATE VIEW dbo.vw_problemas_por_status AS
SELECT
    status,
    COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
GROUP BY status;
GO

-- 2. Problemas agrupados por prioridade
IF OBJECT_ID('dbo.vw_problemas_por_prioridade', 'V') IS NOT NULL
    DROP VIEW dbo.vw_problemas_por_prioridade;
GO

CREATE VIEW dbo.vw_problemas_por_prioridade AS
SELECT
    prioridade,
    COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
GROUP BY prioridade;
GO

-- 3. Problemas agrupados por gerente responsável
IF OBJECT_ID('dbo.vw_problemas_por_gerente', 'V') IS NOT NULL
    DROP VIEW dbo.vw_problemas_por_gerente;
GO

CREATE VIEW dbo.vw_problemas_por_gerente AS
SELECT
    gerente_responsavel,
    COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
GROUP BY gerente_responsavel;
GO

-- 4. Problemas agrupados por departamento relator
IF OBJECT_ID('dbo.vw_problemas_por_departamento', 'V') IS NOT NULL
    DROP VIEW dbo.vw_problemas_por_departamento;
GO

CREATE VIEW dbo.vw_problemas_por_departamento AS
SELECT
    departamento_relator,
    COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
GROUP BY departamento_relator;
GO

-- 5. Problemas agrupados por sistema impactado
IF OBJECT_ID('dbo.vw_problemas_por_sistema', 'V') IS NOT NULL
    DROP VIEW dbo.vw_problemas_por_sistema;
GO

CREATE VIEW dbo.vw_problemas_por_sistema AS
SELECT
    sistema,
    COUNT(*) AS qt_problemas
FROM dbo.tb_problemas_gov_ti
GROUP BY sistema;
GO

-- 6. Problemas abertos (excluídos Resolvido e Cancelado)
IF OBJECT_ID('dbo.vw_problemas_abertos', 'V') IS NOT NULL
    DROP VIEW dbo.vw_problemas_abertos;
GO

CREATE VIEW dbo.vw_problemas_abertos AS
SELECT
    numero,
    prioridade,
    titulo,
    status,
    dt_abertura,
    gerente_responsavel,
    departamento_relator,
    jornada_impactada,
    sistema,
    paliativo,
    impacto
FROM dbo.tb_problemas_gov_ti
WHERE status NOT IN ('Resolvido', 'Cancelado');
GO

-- 7. Pipeline health — métricas do tb_log_etl
IF OBJECT_ID('dbo.vw_pipeline_health', 'V') IS NOT NULL
    DROP VIEW dbo.vw_pipeline_health;
GO

CREATE VIEW dbo.vw_pipeline_health AS
SELECT
    CAST(dt_processamento AS DATE)      AS dt_execucao,
    COUNT(*)                            AS qt_execucoes,
    SUM(CASE WHEN ds_status = 'sucesso'  THEN 1 ELSE 0 END) AS qt_sucesso,
    SUM(CASE WHEN ds_status = 'falha'    THEN 1 ELSE 0 END) AS qt_falha,
    SUM(CASE WHEN ds_status = 'parcial'  THEN 1 ELSE 0 END) AS qt_parcial,
    SUM(qt_linhas_recebidas)            AS total_linhas_recebidas,
    SUM(qt_linhas_inseridas)            AS total_linhas_inseridas,
    SUM(qt_linhas_rejeitadas)           AS total_linhas_rejeitadas,
    AVG(tm_duracao_seg)                 AS media_duracao_seg
FROM dbo.tb_log_etl
GROUP BY CAST(dt_processamento AS DATE);
GO

-- 8. Problemas agrupados por jornada impactada
IF OBJECT_ID('dbo.vw_problemas_por_jornada', 'V') IS NOT NULL
    DROP VIEW dbo.vw_problemas_por_jornada;
GO

CREATE VIEW dbo.vw_problemas_por_jornada AS
SELECT
    ISNULL(jornada_impactada, 'Não informado') AS jornada_impactada,
    COUNT(*) AS qt_problemas,
    SUM(CASE WHEN status NOT IN ('Resolvido','Cancelado') THEN 1 ELSE 0 END) AS qt_abertos
FROM dbo.tb_problemas_gov_ti
GROUP BY jornada_impactada;
GO

-- 9. Problemas agrupados por paliativo (Sim / Não)
IF OBJECT_ID('dbo.vw_problemas_por_paliativo', 'V') IS NOT NULL
    DROP VIEW dbo.vw_problemas_por_paliativo;
GO

CREATE VIEW dbo.vw_problemas_por_paliativo AS
SELECT
    ISNULL(paliativo, 'Não informado') AS paliativo,
    COUNT(*) AS qt_problemas,
    SUM(CASE WHEN status NOT IN ('Resolvido','Cancelado') THEN 1 ELSE 0 END) AS qt_abertos
FROM dbo.tb_problemas_gov_ti
GROUP BY paliativo;
GO

PRINT '9 views KPI criadas/atualizadas com sucesso.';
