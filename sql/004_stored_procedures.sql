-- =============================================================
-- Stored Procedure: sp_merge_problemas_gov_ti
-- Faz MERGE de stg_problemas_gov_ti → tb_problemas_gov_ti
-- Chave de negócio: numero
-- Retorna: @linhas_inseridas, @linhas_atualizadas
-- =============================================================

IF OBJECT_ID('dbo.sp_merge_problemas_gov_ti', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_merge_problemas_gov_ti;
GO

CREATE PROCEDURE dbo.sp_merge_problemas_gov_ti
    @linhas_inseridas   INT OUTPUT,
    @linhas_atualizadas INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @acao TABLE (tipo_acao VARCHAR(10));

    MERGE dbo.tb_problemas_gov_ti AS destino
    USING (
        SELECT
            numero,
            prioridade,
            titulo,
            descricao,
            dt_abertura,
            dt_conclusao,
            status,
            gerente_responsavel,
            departamento_relator,
            jornada_impactada,
            sistema,
            paliativo,
            impacto,
            status_14_04,
            status_17_04,
            status_22_04,
            nm_arquivo_origem
        FROM dbo.stg_problemas_gov_ti
        WHERE numero IS NOT NULL
    ) AS origem
    ON destino.numero = origem.numero

    WHEN MATCHED THEN
        UPDATE SET
            destino.prioridade           = origem.prioridade,
            destino.titulo               = origem.titulo,
            destino.descricao            = origem.descricao,
            destino.dt_abertura          = origem.dt_abertura,
            destino.dt_conclusao         = origem.dt_conclusao,
            destino.status               = origem.status,
            destino.gerente_responsavel  = origem.gerente_responsavel,
            destino.departamento_relator = origem.departamento_relator,
            destino.jornada_impactada    = origem.jornada_impactada,
            destino.sistema              = origem.sistema,
            destino.paliativo            = origem.paliativo,
            destino.impacto              = origem.impacto,
            destino.status_14_04         = origem.status_14_04,
            destino.status_17_04         = origem.status_17_04,
            destino.status_22_04         = origem.status_22_04,
            destino.nm_arquivo_origem    = origem.nm_arquivo_origem,
            destino.dt_atualizacao       = GETDATE()

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            numero, prioridade, titulo, descricao,
            dt_abertura, dt_conclusao, status,
            gerente_responsavel, departamento_relator,
            jornada_impactada, sistema, paliativo,
            impacto, status_14_04, status_17_04, status_22_04,
            nm_arquivo_origem, dt_insert
        )
        VALUES (
            origem.numero, origem.prioridade, origem.titulo, origem.descricao,
            origem.dt_abertura, origem.dt_conclusao, origem.status,
            origem.gerente_responsavel, origem.departamento_relator,
            origem.jornada_impactada, origem.sistema, origem.paliativo,
            origem.impacto, origem.status_14_04, origem.status_17_04, origem.status_22_04,
            origem.nm_arquivo_origem, GETDATE()
        )

    OUTPUT $action INTO @acao;

    SELECT @linhas_inseridas   = COUNT(*) FROM @acao WHERE tipo_acao = 'INSERT';
    SELECT @linhas_atualizadas = COUNT(*) FROM @acao WHERE tipo_acao = 'UPDATE';
END;
GO

PRINT 'SP sp_merge_problemas_gov_ti criada/atualizada com sucesso.';
