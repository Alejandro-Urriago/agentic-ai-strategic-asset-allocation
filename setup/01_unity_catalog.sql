-- One-time Unity Catalog setup for the dev workspace (idempotent).
-- Catalog is the auto-created workspace catalog.
CREATE SCHEMA IF NOT EXISTS adb_alejandro_research_dev_aue_ws.landing COMMENT 'Raw files (volumes) before ingestion';
CREATE SCHEMA IF NOT EXISTS adb_alejandro_research_dev_aue_ws.control COMMENT 'Pipeline control tables';
CREATE SCHEMA IF NOT EXISTS adb_alejandro_research_dev_aue_ws.bronze  COMMENT 'Raw tables loaded by SDP';
CREATE SCHEMA IF NOT EXISTS adb_alejandro_research_dev_aue_ws.silver  COMMENT 'Cleaned models (dbt)';
CREATE SCHEMA IF NOT EXISTS adb_alejandro_research_dev_aue_ws.gold    COMMENT 'Business models (dbt)';

-- One volume per source system (= pipeline_group in config/sources/*.json).
CREATE VOLUME IF NOT EXISTS adb_alejandro_research_dev_aue_ws.landing.apra COMMENT 'Modelled APRA SAA CSVs landed for bronze ingestion';
