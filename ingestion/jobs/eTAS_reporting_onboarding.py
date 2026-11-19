"""eTAS - Onboarding (APRA research version, Python file)

Same structure as the WRHS notebook eTAS_WRHS_reporting_onboarding, using the 4 APRA tables.
Builds the 2 dlt-meta control tables <catalog>.onboarding.bronze_dataflowspec and silver_dataflowspec.

Refresh properties (bronze_table_properties and silver_table_properties, same values):
    dim_investment_option                 full_override true  (plays the role of Workday), group ""
    fact_investment_strategic_allocation  full_override false, group finance_daily (plays the role of finance)
    dim_asset_class, dim_fund             full_override false, group ""

Bronze -> <catalog>.raw_delta.bronze_<table>  (append, Auto Loader)
Silver -> <catalog>.curated.silver_<table>    (SCD Type 2, like WRHS)
  LoadDateTime = current_timestamp() added in the silver select (APRA files have no load timestamp).
  It is excluded from history tracking (track_history_except_column_list), so a reload of
  identical rows does not create new history versions (WRHS solution 3).

Job task (Python file). Task environment dependency: dlt-meta==0.0.10
Parameters: --catalog <catalog> --raw-root <landing root>
"""
import argparse
import json

from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--raw-root", required=True, help="landing root, e.g. /Volumes/<catalog>/<schema>/<volume>")
parser.add_argument("--curstorage", default="", help="where config files are written (default: onboarding.cur volume)")
args = parser.parse_args()
catalog = args.catalog
rawroot = args.raw_root.rstrip("/")

# =====================================================================
# 1.0 Core Config
# =====================================================================

# 1.1 Install DLT-Meta
# WRHS: %pip install dlt-meta==0.0.10. Here: task environment dependency dlt-meta==0.0.10

# 1.2 Setup Schemas
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql("create schema if not exists raw_delta")
spark.sql("create schema if not exists curated")
spark.sql("create schema if not exists onboarding")
spark.sql("CREATE VOLUME IF NOT EXISTS onboarding.cur")
curstorage = args.curstorage or f"/Volumes/{catalog}/onboarding/cur"

# =====================================================================
# 2.0 Ingestion Config
# =====================================================================
onboarding_json = "["
silver_transformations_json = "["
dqe = {}   # data quality rules per table (from the "expectations" in config/sources)

# ---------------------------------------------------------------------
# 2.1 Table Onboarding
# ---------------------------------------------------------------------

# Asset class hierarchy (sector_l1, listing, domicile); keys 101-231 map SAA sectors.
onboarding_json += f"""
    \u007b
        "data_flow_id": "500",
        "data_flow_group": "apra",
        "source_system": "apra",
        "source_format": "cloudFiles",
        "source_details": \u007b
            "source_database": "landing",
            "source_table": "dim_asset_class",
            "source_path_prod": "{rawroot}/apra/dim_asset_class"
        \u007d,
        "bronze_database_prod": "raw_delta",
        "bronze_table": "bronze_dim_asset_class",
        "bronze_table_comment": "Asset class hierarchy (sector_l1, listing, domicile); keys 101-231 map SAA sectors.",
        "bronze_table_properties": \u007b
            "refresh.full_override": "false",
            "refresh.group": "",
            "delta.enableChangeDataFeed": "true"
        \u007d,
        "bronze_reader_options": \u007b
            "cloudFiles.format": "csv",
            "header": "true",
            "rescuedDataColumn": "_rescued_data"
        \u007d,
        "bronze_data_quality_expectations_json_prod": "{curstorage}/etas/configuration/dqe/apra/dim_asset_class.json",
        "bronze_database_quarantine_prod": "raw_delta",
        "bronze_quarantine_table": "bronze_dim_asset_class_quarantine",
        "silver_database_prod": "curated",
        "silver_table": "silver_dim_asset_class",
        "silver_table_properties": \u007b
            "refresh.full_override": "false",
            "refresh.group": ""
        \u007d,
        "silver_cdc_apply_changes": \u007b
            "keys": ["asset_class_key"],
            "sequence_by": "LoadDateTime",
            "scd_type": "2",
            "except_column_list": ["_rescued_data"],
            "track_history_except_column_list": ["LoadDateTime"]
        \u007d,
        "silver_transformation_json_prod": "{curstorage}/etas/configuration/silver_transformations.json"
    \u007d,
"""

silver_transformations_json += """
  {
    "target_table": "silver_dim_asset_class",
    "select_exp": [
      "asset_class_key",
      "asset_class_name",
      "sector_l1",
      "sub_sector_l2",
      "listing_type",
      "domicile_type",
      "economy_type",
      "hedging_status",
      "measure_type",
      "parent_asset_class",
      "is_overlay",
      "source_system",
      "source_column",
      "saa_sector_type",
      "_rescued_data",
      "current_timestamp() as LoadDateTime"
    ]
  },
"""
dqe["dim_asset_class"] = {"asset_class_key_present": "asset_class_key IS NOT NULL", "is_overlay_boolean": "is_overlay IS NULL OR lower(is_overlay) IN ('true','false')", "no_rescued_data": "_rescued_data IS NULL"}

# Funds compared in the project (10 RSEs).
onboarding_json += f"""
    \u007b
        "data_flow_id": "501",
        "data_flow_group": "apra",
        "source_system": "apra",
        "source_format": "cloudFiles",
        "source_details": \u007b
            "source_database": "landing",
            "source_table": "dim_fund",
            "source_path_prod": "{rawroot}/apra/dim_fund"
        \u007d,
        "bronze_database_prod": "raw_delta",
        "bronze_table": "bronze_dim_fund",
        "bronze_table_comment": "Funds compared in the project (10 RSEs).",
        "bronze_table_properties": \u007b
            "refresh.full_override": "false",
            "refresh.group": "",
            "delta.enableChangeDataFeed": "true"
        \u007d,
        "bronze_reader_options": \u007b
            "cloudFiles.format": "csv",
            "header": "true",
            "rescuedDataColumn": "_rescued_data"
        \u007d,
        "bronze_data_quality_expectations_json_prod": "{curstorage}/etas/configuration/dqe/apra/dim_fund.json",
        "bronze_database_quarantine_prod": "raw_delta",
        "bronze_quarantine_table": "bronze_dim_fund_quarantine",
        "silver_database_prod": "curated",
        "silver_table": "silver_dim_fund",
        "silver_table_properties": \u007b
            "refresh.full_override": "false",
            "refresh.group": ""
        \u007d,
        "silver_cdc_apply_changes": \u007b
            "keys": ["fund_key"],
            "sequence_by": "LoadDateTime",
            "scd_type": "2",
            "except_column_list": ["_rescued_data"],
            "track_history_except_column_list": ["LoadDateTime"]
        \u007d,
        "silver_transformation_json_prod": "{curstorage}/etas/configuration/silver_transformations.json"
    \u007d,
"""

silver_transformations_json += """
  {
    "target_table": "silver_dim_fund",
    "select_exp": [
      "fund_key",
      "rse_abn",
      "fund_name",
      "short_name",
      "fund_type",
      "_rescued_data",
      "current_timestamp() as LoadDateTime"
    ]
  },
"""
dqe["dim_fund"] = {"fund_key_present": "fund_key IS NOT NULL", "rse_abn_present": "rse_abn IS NOT NULL", "no_rescued_data": "_rescued_data IS NULL"}

# Largest diversified investment option per fund per risk profile (from QSPS Tables 8a-8d member assets). Each file is the full list; the latest file replaces the previous one.
onboarding_json += f"""
    \u007b
        "data_flow_id": "502",
        "data_flow_group": "apra",
        "source_system": "apra",
        "source_format": "cloudFiles",
        "source_details": \u007b
            "source_database": "landing",
            "source_table": "dim_investment_option",
            "source_path_prod": "{rawroot}/apra/dim_investment_option"
        \u007d,
        "bronze_database_prod": "raw_delta",
        "bronze_table": "bronze_dim_investment_option",
        "bronze_table_comment": "Largest diversified investment option per fund per risk profile (from QSPS Tables 8a-8d member assets). Each file is the full list; the latest file replaces the previous one.",
        "bronze_table_properties": \u007b
            "refresh.full_override": "true",
            "refresh.group": "",
            "delta.enableChangeDataFeed": "true"
        \u007d,
        "bronze_reader_options": \u007b
            "cloudFiles.format": "csv",
            "header": "true",
            "rescuedDataColumn": "_rescued_data"
        \u007d,
        "bronze_data_quality_expectations_json_prod": "{curstorage}/etas/configuration/dqe/apra/dim_investment_option.json",
        "bronze_database_quarantine_prod": "raw_delta",
        "bronze_quarantine_table": "bronze_dim_investment_option_quarantine",
        "silver_database_prod": "curated",
        "silver_table": "silver_dim_investment_option",
        "silver_table_properties": \u007b
            "refresh.full_override": "true",
            "refresh.group": ""
        \u007d,
        "silver_cdc_apply_changes": \u007b
            "keys": ["investment_option_key"],
            "sequence_by": "LoadDateTime",
            "scd_type": "2",
            "except_column_list": ["_rescued_data"],
            "track_history_except_column_list": ["LoadDateTime"]
        \u007d,
        "silver_transformation_json_prod": "{curstorage}/etas/configuration/silver_transformations.json"
    \u007d,
"""

silver_transformations_json += """
  {
    "target_table": "silver_dim_investment_option",
    "select_exp": [
      "investment_option_key",
      "investment_option_id",
      "investment_option_name",
      "investment_option_type",
      "investment_option_category",
      "fund_key",
      "risk_profile",
      "growth_assets_pct",
      "defensive_assets_pct",
      "total_member_assets",
      "_rescued_data",
      "current_timestamp() as LoadDateTime"
    ]
  },
"""
dqe["dim_investment_option"] = {"option_key_present": "investment_option_key IS NOT NULL", "fund_key_present": "fund_key IS NOT NULL", "risk_profile_present": "risk_profile IS NOT NULL", "no_rescued_data": "_rescued_data IS NULL"}

# Board-approved SAA target allocation % by date, fund, option and asset class (APRA QSPS Historical SAA). Not actual holdings.
onboarding_json += f"""
    \u007b
        "data_flow_id": "503",
        "data_flow_group": "apra",
        "source_system": "apra",
        "source_format": "cloudFiles",
        "source_details": \u007b
            "source_database": "landing",
            "source_table": "fact_investment_strategic_allocation",
            "source_path_prod": "{rawroot}/apra/fact_investment_strategic_allocation"
        \u007d,
        "bronze_database_prod": "raw_delta",
        "bronze_table": "bronze_fact_investment_strategic_allocation",
        "bronze_table_comment": "Board-approved SAA target allocation % by date, fund, option and asset class (APRA QSPS Historical SAA). Not actual holdings.",
        "bronze_table_properties": \u007b
            "refresh.full_override": "false",
            "refresh.group": "finance_daily",
            "delta.enableChangeDataFeed": "true"
        \u007d,
        "bronze_reader_options": \u007b
            "cloudFiles.format": "csv",
            "header": "true",
            "rescuedDataColumn": "_rescued_data"
        \u007d,
        "bronze_data_quality_expectations_json_prod": "{curstorage}/etas/configuration/dqe/apra/fact_investment_strategic_allocation.json",
        "bronze_database_quarantine_prod": "raw_delta",
        "bronze_quarantine_table": "bronze_fact_investment_strategic_allocation_quarantine",
        "silver_database_prod": "curated",
        "silver_table": "silver_fact_investment_strategic_allocation",
        "silver_table_properties": \u007b
            "refresh.full_override": "false",
            "refresh.group": "finance_daily"
        \u007d,
        "silver_cdc_apply_changes": \u007b
            "keys": ["date_key", "fund_key", "investment_option_key", "asset_class_key"],
            "sequence_by": "LoadDateTime",
            "scd_type": "2",
            "except_column_list": ["_rescued_data"],
            "track_history_except_column_list": ["LoadDateTime"]
        \u007d,
        "silver_transformation_json_prod": "{curstorage}/etas/configuration/silver_transformations.json"
    \u007d,
"""

silver_transformations_json += """
  {
    "target_table": "silver_fact_investment_strategic_allocation",
    "select_exp": [
      "date_key",
      "fund_key",
      "investment_option_key",
      "asset_class_key",
      "benchmark_allocation_pct",
      "currency_hedging_ratio_pct",
      "_rescued_data",
      "current_timestamp() as LoadDateTime"
    ]
  },
"""
dqe["fact_investment_strategic_allocation"] = {"date_key_present": "date_key IS NOT NULL", "fund_key_present": "fund_key IS NOT NULL", "option_key_present": "investment_option_key IS NOT NULL", "asset_class_key_present": "asset_class_key IS NOT NULL", "pct_is_numeric_or_null": "benchmark_allocation_pct IS NULL OR try_cast(benchmark_allocation_pct AS DOUBLE) IS NOT NULL", "no_rescued_data": "_rescued_data IS NULL"}

# ---------------------------------------------------------------------
# 2.2 Build Config Files
# ---------------------------------------------------------------------
onboarding_json += "]"
onboarding_json = onboarding_json.replace("},\n]", "}\n]")   # drop the last comma
silver_transformations_json += "]"
silver_transformations_json = silver_transformations_json.replace("},\n]", "}\n]")

dbutils.fs.put(f"{curstorage}/etas/configuration/silver_transformations.json", silver_transformations_json, True)

dbutils.fs.put(f"{curstorage}/etas/configuration/onboarding.json", onboarding_json, True)
# "expect" = check rows in the main table (keep them). "expect_or_quarantine" = also create the
# <table>_quarantine table, like WRHS (dlt-meta only creates it when this key exists)
for sid, rules in dqe.items():
    dbutils.fs.put(f"{curstorage}/etas/configuration/dqe/apra/{sid}.json", json.dumps({"expect": rules, "expect_or_quarantine": rules}, indent=2), True)

# =====================================================================
# 3.0 Build Dataflows
# =====================================================================

# 3.1 Load Configuration Files (Bronze + Silver, like WRHS: onboard_dataflow_specs)
from src.onboard_dataflowspec import OnboardDataflowspec

onboarding_params_map = {
    "onboarding_file_path": f"{curstorage}/etas/configuration/onboarding.json",
    "database": f"{catalog}.onboarding",
    "env": "prod",
    "bronze_dataflowspec_table": "bronze_dataflowspec",
    "silver_dataflowspec_table": "silver_dataflowspec",
    "overwrite": "True",
    "version": "v1",
    "import_author": "alejandro",
}
print(onboarding_params_map)
OnboardDataflowspec(spark, onboarding_params_map, uc_enabled=True).onboard_dataflow_specs()

# 3.1 Verify Configuration
for layer in ("bronze", "silver"):
    print(f"--- onboarding.{layer}_dataflowspec")
    for r in spark.sql(f"select dataFlowId, targetDetails['table'] as table_name, tableProperties "
                       f"from onboarding.{layer}_dataflowspec order by dataFlowId").collect():
        print(r.dataFlowId, r.table_name, r.tableProperties)
