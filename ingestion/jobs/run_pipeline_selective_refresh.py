"""Run a Lakeflow pipeline, refreshing only selected tables.

Which tables get a full refresh is decided by a Unity Catalog tag on each table:
  refresh_mode = full   -> table is wiped and reloaded from all files
  (no tag)              -> normal refresh, only new files are loaded

The pipeline's tables are taken from the pipeline's default catalog and schema,
so this assumes that schema only holds this pipeline's tables.
"""
import time

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

TAG_NAME = "refresh_mode"
TAG_VALUE_FULL = "full"

# Connect to the workspace and to Spark (uses the job's login, no password needed)
w = WorkspaceClient()
spark = SparkSession.builder.getOrCreate()

# Pipeline to run (bronze_pipeline)
pipeline_id = "a5ffa9d3-b81c-46e3-a49b-9e286612a62a"

# 1. Find the pipeline's default catalog and schema (from its settings)
spec = w.pipelines.get(pipeline_id=pipeline_id).spec
catalog = spec.catalog
schema = spec.schema or spec.target
print(f"Pipeline writes to: {catalog}.{schema}")

# 2. All tables in that schema = the pipeline's tables
all_tables = [
    row.table_name
    for row in spark.sql(f"""
        SELECT table_name
        FROM {catalog}.information_schema.tables
        WHERE table_schema = '{schema}'
    """).collect()
]

# 3. Tables tagged refresh_mode = full
tagged_full = [
    row.table_name
    for row in spark.sql(f"""
        SELECT table_name
        FROM {catalog}.information_schema.table_tags
        WHERE schema_name = '{schema}'
          AND tag_name = '{TAG_NAME}'
          AND tag_value = '{TAG_VALUE_FULL}'
    """).collect()
]

# 4. Build the two lists with full names (catalog.schema.table)
tables_full_refresh = [f"{catalog}.{schema}.{t}" for t in all_tables if t in tagged_full]
tables_normal_refresh = [f"{catalog}.{schema}.{t}" for t in all_tables if t not in tagged_full]
print(f"Full refresh:   {tables_full_refresh}")
print(f"Normal refresh: {tables_normal_refresh}")

# Reference: the earlier hard-coded version of the two lists (step 1 of the proof of concept).
# Steps 1 to 4 above now build the same lists from the refresh_mode tag instead.
#
# tables_full_refresh = [
#     "adb_alejandro_research_dev_aue_ws.bronze.bronze_dim_asset_class",
# ]
#
# tables_normal_refresh = [
#     "adb_alejandro_research_dev_aue_ws.bronze.bronze_dim_fund",
#     "adb_alejandro_research_dev_aue_ws.bronze.bronze_dim_investment_option",
#     "adb_alejandro_research_dev_aue_ws.bronze.bronze_fact_investment_strategic_allocation",
# ]

# 5. Start the pipeline with the two lists
update = w.pipelines.start_update(
    pipeline_id=pipeline_id,
    full_refresh_selection=tables_full_refresh,
    refresh_selection=tables_normal_refresh,
)
print(f"Started update {update.update_id}")

# 6. Wait until the pipeline run finishes, checking every 30 seconds
while True:
    info = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update.update_id)
    state = info.update.state.value
    print(state)
    if state in ("COMPLETED", "FAILED", "CANCELED"):
        break
    time.sleep(30)

# 7. Stop the job if the pipeline did not finish well, so later steps do not run
if state != "COMPLETED":
    raise Exception(f"Pipeline ended with state {state}")

print("Pipeline finished")