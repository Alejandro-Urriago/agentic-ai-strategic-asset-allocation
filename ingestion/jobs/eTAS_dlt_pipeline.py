# Databricks notebook source
#%pip install dlt-meta
# PINNED - dlt-meta 0.1.0 (released 31 Aug 2026) breaks this pipeline; do not remove the version pin
%pip install dlt-meta==0.0.10 databricks-sdk #2026-09-04

# COMMAND ----------

# DBTITLE 0,Widgets
layer = spark.conf.get("layer", None)

from src.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)