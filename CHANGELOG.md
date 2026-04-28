# Changelog

## [1.0.0] — 2026-04-25

### Added
- Watcher automático com `watchdog` para monitorar pasta de entrada
- ETL completo: leitura Excel → staging → produção via MERGE
- Módulo de alertas: Telegram imediato + Email diário via APScheduler
- Logger estruturado em arquivo e banco (`tb_log_etl`)
- Scripts SQL: staging, produção, log, stored procedures, views KPI
- 7 views KPI para consumo no Metabase
- Script CLI `reprocessar.py` para reprocessamento manual
- Suporte a múltiplas tabelas via `config/tabelas.json`
