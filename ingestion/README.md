# Metadata-driven bronze ingestion (not yet run on Databricks)

Designed to scale to hundreds of sources: Git is the source of truth, a Delta control table is what the pipeline reads.

```
config/sources/<source_id>.json   (one file per source, reviewed via PR)
        |  CI: sync_control_table.py --validate-only
        |  deploy job: sync_control_table.py  (MERGE)
        v
<catalog>.control.bronze_sources        (Delta control table)
        |  read at pipeline start, filtered by pipeline_group + enabled
        v
SDP pipeline per pipeline_group -> one Auto Loader streaming table per source
        v
dbt sources (silver/gold in dbt)
```

## Files
- `config/sources/*.json`: one file per source (`source_id`, `target_table`, `pipeline_group`, `owner`, `path`, `format`, `options`, `expectations`, `enabled`).
- `jobs/sync_control_table.py`: validates all files (required fields, types, snake_case names, unique ids/targets, allowed formats, file name = source_id); MERGEs into the control table. Only changed rows are updated (`config_hash`). A removed file sets `enabled=false, is_deleted=true`; rows are never deleted.
- `transformations/bronze_ingest.py`: reads enabled sources for its `pipeline_group` from the control table; a factory function registers one streaming table per source with `_source_id`, `_source_file`, `_source_file_modified_at`, `_ingested_at`; expectations warn only.
- `../dbt/models/staging/_bronze__sources.yml`: dbt sources with quarterly freshness.

## How it scales
- New source: add one JSON file + PR. No code change.
- Many sources: split into several pipelines via `pipeline_group` (e.g. `apra`, `market_data`), each reading its slice of the same control table.
- Emergency pause: `UPDATE control.bronze_sources SET enabled = false WHERE source_id = '...'` (next Git sync restores the Git value, so follow up with a PR).
- History: Git (who/why, reviewed) + `updated_at`/`updated_by` + Delta `DESCRIBE HISTORY` on the control table.
- Loaded-file tracking: Auto Loader checkpoint, queryable with `cloud_files_state(TABLE(<bronze table>))`. No state table needed.

## Setup
0. Run `../setup/01_unity_catalog.sql` once (schemas + volume).
1. Landing folders are created by the sync job from the config (`--raw-root /Volumes/<catalog>/landing` -> `<raw_root>/<pipeline_group>/<path>`). (The 4 APRA folders were created manually once during initial setup.)
2. Job task 1: `sync_control_table.py --config-dir config/sources --control-table <catalog>.control.bronze_sources --raw-root /Volumes/<catalog>/landing --updated-by <git sha>`.
3. Job task 2: the pipeline (serverless). Configuration: `bronze.control_table`, `bronze.pipeline_group`, `bronze.raw_root`.
4. CI on every PR: `.github/workflows/validate-ingestion-config.yml` runs `--validate-only`.

## Assumptions to verify on first run
- Reading the control table with `collect()` at module level (outside dataset functions) in the pipeline source. Spark docs warn against `collect()` inside dataset code; module level should be fine but is unconfirmed.
- Pipeline graph is built at update start, so control-table changes apply on the next pipeline update.
- dbt `freshness` / `loaded_at_field` placement may need `config:` in newer dbt versions.
- At scale, `_bronze__sources.yml` could be generated from the control table instead of hand-written.
