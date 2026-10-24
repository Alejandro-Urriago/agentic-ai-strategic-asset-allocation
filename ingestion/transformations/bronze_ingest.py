"""Metadata-driven bronze ingestion (Lakeflow Spark Declarative Pipelines).

Reads the control table (updated from Git by jobs/update_control_table.py) and
registers one Auto Loader streaming table per enabled source in this
pipeline's group. Adding a source = adding a config file in Git, no code change.

Pipeline configuration keys (set in the pipeline's Configuration field):
  bronze.control_table   e.g. <catalog>.control.bronze_sources
  bronze.pipeline_group  e.g. apra  (one pipeline per group scales to many sources)
  bronze.raw_root        e.g. /Volumes/<catalog>/landing  (files at <raw_root>/<pipeline_group>/<path>)
"""
import re

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CONTROL_TABLE = spark.conf.get("bronze.control_table")
PIPELINE_GROUP = spark.conf.get("bronze.pipeline_group")
RAW_ROOT = spark.conf.get("bronze.raw_root").rstrip("/")

INVALID_CHARS = re.compile(r"[ ,;{}()\n\t=]+")

# Read once at graph-definition time, outside any dataset function.
SOURCES = (
    spark.read.table(CONTROL_TABLE)
    .where(F.col("pipeline_group") == PIPELINE_GROUP)
    .where(F.col("enabled"))
    .collect()
)


def create_bronze_table(src):
    """Factory: each call registers one streaming table with its own parameters."""

    # Expectations use warn (expect_all), not drop, so bronze keeps every row.
    @dp.table(
        name=src["target_table"],
        comment=src["description"],
        table_properties={
            "quality": "bronze",
            "source_id": src["source_id"],
            "owner": src["owner"],
            "config_hash": src["config_hash"],
        },
    )
    @dp.expect_all(src["expectations"] or {})
    def bronze():
        reader = spark.readStream.format("cloudFiles").option(
            "cloudFiles.format", src["format"]
        )
        for key, value in (src["options"] or {}).items():
            reader = reader.option(key, value)
        df = (
            reader.load(f"{RAW_ROOT}/{src['pipeline_group']}/{src['path']}")
            .withColumn("_source_id", F.lit(src["source_id"]))
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_source_file_modified_at", F.col("_metadata.file_modification_time"))
            .withColumn("_ingested_at", F.current_timestamp())
        )
        # Delta rejects spaces and ,;{}()=\n\t in column names, and some APRA
        # files start with a UTF-8 BOM; normalise names after reading _metadata.
        return df.toDF(
            *[INVALID_CHARS.sub("_", c.lstrip("\ufeff").strip()) for c in df.columns]
        )


for row in SOURCES:
    create_bronze_table(row.asDict())
