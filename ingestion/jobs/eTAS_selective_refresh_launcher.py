# Databricks notebook source
# MAGIC %pip install -q databricks-sdk==0.40.0

# COMMAND ----------

"""eTAS selective refresh launcher (dlt-meta 0.0.10) - NEW file

Runs in the job tasks bronze_layer (layer = bronze) and silver_layer (layer = silver).
Reads the tables and their properties from the dataflowspec control table, then starts
ONE update of that layer's existing pipeline (the pipeline still runs eTAS_dlt_pipeline).

Parameters (task parameters, key = value):
  layer          bronze | silver                  (fixed in each task)
  pipeline_id    id of that layer's pipeline      (fixed in each task)
  refresh_group  {{job.parameters.refresh_group}} (job parameter, default all)
  spec_schema    schema of the dataflowspec tables (e.g. <catalog>.onboarding)

Which tables (refresh_group):
  all            -> every table of the layer (overnight run)
  finance_daily  -> only tables with property refresh.group = finance_daily (midday run)

Which mode (property refresh.full_override):
  true           -> FULL refresh (Workday)
  not set        -> NORMAL (incremental) refresh
"""
import time

from databricks.sdk import WorkspaceClient

GROUP_KEY = "refresh.group"
FULL_KEY = "refresh.full_override"
BUSY_STATES = {"DEPLOYING", "STARTING", "RUNNING", "STOPPING", "RECOVERING", "RESETTING"}
MAX_WAIT_IF_BUSY_MIN = 60   # wait this long if the pipeline is already running
POLL_SECONDS = 30

# ---------- 1. Parameters ----------
dbutils.widgets.text("layer", "")
dbutils.widgets.text("pipeline_id", "")
dbutils.widgets.text("refresh_group", "all")
dbutils.widgets.text("spec_schema", "hive_metastore.onboarding")

layer = dbutils.widgets.get("layer").strip().lower()
pipeline_id = dbutils.widgets.get("pipeline_id").strip()
refresh_group = dbutils.widgets.get("refresh_group").strip() or "all"
spec_schema = dbutils.widgets.get("spec_schema").strip()

if layer not in ("bronze", "silver") or not pipeline_id:
    raise Exception("Set task parameters layer (bronze/silver) and pipeline_id")

# Log in with the notebook's own login (default login fails on Shared Cluster, seen 10 Nov)
try:
    w = WorkspaceClient()
    w.current_user.me()
except Exception:
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    w = WorkspaceClient(host=ctx.apiUrl().get(), token=ctx.apiToken().get())

# ---------- 2. Read the tables and their tags from the dataflowspec ----------
spec = spark.table(f"{spec_schema}.{layer}_dataflowspec")
has_quarantine = "quarantineTargetDetails" in spec.columns
rows = spec.collect()


def props(row):
    return row.tableProperties or {}


if refresh_group.lower() == "all":
    selected = rows
else:
    selected = [r for r in rows if props(r).get(GROUP_KEY) == refresh_group]

# ---------- 3. Build the two lists (main table + its quarantine table) ----------
tables_full_refresh, tables_normal_refresh = [], []
qualified_full, qualified_normal = [], []   # DEMO: same tables as database.table (fallback below)
for r in selected:
    names = [r.targetDetails["table"]]
    qnames = [f'{r.targetDetails["database"]}.{r.targetDetails["table"]}']
    if has_quarantine and r.quarantineTargetDetails:
        names.append(r.quarantineTargetDetails["table"])
        qnames.append(f'{r.quarantineTargetDetails["database"]}.{r.quarantineTargetDetails["table"]}')
    is_full = str(props(r).get(FULL_KEY, "")).lower() == "true"
    (tables_full_refresh if is_full else tables_normal_refresh).extend(names)
    (qualified_full if is_full else qualified_normal).extend(qnames)

print(f"Layer: {layer} | refresh_group: {refresh_group}")
print(f"Full refresh ({len(tables_full_refresh)}): {tables_full_refresh}")
print(f"Normal refresh ({len(tables_normal_refresh)}): {tables_normal_refresh}")
if not tables_full_refresh and not tables_normal_refresh:
    raise Exception(f"No tables tagged {GROUP_KEY} = {refresh_group} in {layer}_dataflowspec")

# ---------- 4. If the pipeline is busy (e.g. overnight run), wait ----------
deadline = time.time() + MAX_WAIT_IF_BUSY_MIN * 60
while True:
    state = w.pipelines.get(pipeline_id=pipeline_id).state
    state = state.value if state else "IDLE"
    if state not in BUSY_STATES:
        break
    if time.time() > deadline:
        raise Exception(f"Pipeline still {state} after {MAX_WAIT_IF_BUSY_MIN} min, not started")
    print(f"Pipeline is {state}, waiting...")
    time.sleep(POLL_SECONDS)

# ---------- 5-7. Start the update, wait, fail the task unless COMPLETED ----------
# DEMO: dlt-meta 0.0.10 names Bronze tables "table" but SCD2 Silver tables "database.table".
# Try 1 = plain names (WRHS launcher). If that fails, try 2 = database.table. The output says which worked.
def run_update(full_list, normal_list, label):
    try:
        update = w.pipelines.start_update(
            pipeline_id=pipeline_id,
            full_refresh_selection=full_list or None,
            refresh_selection=normal_list or None,
        )
    except Exception as e:
        print(f"[{label}] start_update rejected: {e}")
        return "REJECTED"
    print(f"[{label}] Started update {update.update_id}")
    while True:
        state = w.pipelines.get_update(
            pipeline_id=pipeline_id, update_id=update.update_id
        ).update.state.value
        print(f"[{label}] State: {state}")
        if state in ("COMPLETED", "FAILED", "CANCELED"):
            return state
        time.sleep(POLL_SECONDS)

state = run_update(tables_full_refresh, tables_normal_refresh, "names: table")
if state != "COMPLETED":
    state = run_update(qualified_full, qualified_normal, "names: database.table")

if state != "COMPLETED":
    raise Exception(f"Pipeline update ended with state {state}")
