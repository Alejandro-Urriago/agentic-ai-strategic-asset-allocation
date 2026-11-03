"""Run a Lakeflow pipeline, refreshing only selected tables.

Step 1 of the proof of concept: table names are written in this file (hard-coded).
Next step: pass the pipeline ID and table lists in as task parameters instead.

- tables_full_refresh:   tables rebuilt from zero (data and memory cleared)
- tables_normal_refresh: tables that only process new data
"""
import time

from databricks.sdk import WorkspaceClient

# Connect to the workspace (uses the job's login, no password needed)
w = WorkspaceClient()

# Pipeline to run (bronze_pipeline)
pipeline_id = "a5ffa9d3-b81c-46e3-a49b-9e286612a62a"

# Tables to rebuild from zero
tables_full_refresh = [
    "adb_alejandro_research_dev_aue_ws.bronze.bronze_dim_asset_class",
]

# Tables to process normally (only new data)
tables_normal_refresh = [
    "adb_alejandro_research_dev_aue_ws.bronze.bronze_dim_fund",
    "adb_alejandro_research_dev_aue_ws.bronze.bronze_dim_investment_option",
    "adb_alejandro_research_dev_aue_ws.bronze.bronze_fact_investment_strategic_allocation",
]

# Start the pipeline with the two lists
update = w.pipelines.start_update(
    pipeline_id=pipeline_id,
    full_refresh_selection=tables_full_refresh,
    refresh_selection=tables_normal_refresh,
)
print(f"Started update {update.update_id}")

# Wait until the pipeline run finishes, checking every 30 seconds
while True:
    info = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update.update_id)
    state = info.update.state.value
    print(state)
    if state in ("COMPLETED", "FAILED", "CANCELED"):
        break
    time.sleep(30)

# Stop the job if the pipeline did not finish well, so later steps do not run
if state != "COMPLETED":
    raise Exception(f"Pipeline ended with state {state}")

print("Pipeline finished")