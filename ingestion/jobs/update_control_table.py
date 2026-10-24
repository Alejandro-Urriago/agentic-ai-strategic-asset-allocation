"""Update the bronze control table from the per-source config files in Git.

Git is the source of truth (one JSON file per source, reviewed via pull request).
CI runs `--validate-only` on every PR; the job runs the full update, which
MERGEs the files into a Delta control table that the pipeline reads at runtime.

Usage:
  python update_control_table.py --config-dir config/sources --validate-only
  (Databricks job task) --config-dir ... --control-table main.control.bronze_sources --updated-by <git sha or CI user>
"""
import argparse
import glob
import hashlib
import json
import os
import re
import sys

REQUIRED = {
    "source_id": str,
    "target_table": str,
    "pipeline_group": str,
    "owner": str,
    "description": str,
    "path": str,
    "format": str,
    "options": dict,
    "expectations": dict,
    "enabled": bool,
}
ALLOWED_FORMATS = {"csv", "json", "parquet", "avro", "orc", "text", "binaryFile", "xml"}
IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")


def resolve_config_dir(config_dir):
    """Return the config folder path, whatever folder the job starts in.

    A relative path (e.g. ingestion/config/sources) is read from the repo root.
    The repo root is two folders up from this script (ingestion/jobs/).
    """
    if os.path.isabs(config_dir) or os.path.isdir(config_dir):
        return config_dir
    script = globals().get("__file__") or sys.argv[0]
    repo_root = os.path.abspath(os.path.join(os.path.dirname(script), "..", ".."))
    return os.path.join(repo_root, config_dir)


def load_and_validate(config_dir):
    """Return the list of source configs, or exit with every problem found."""
    files = sorted(glob.glob(os.path.join(config_dir, "*.json")))
    errors, sources, seen_ids, seen_targets = [], [], set(), set()
    if not files:
        errors.append(f"no config files in {config_dir}")

    for file in files:
        try:
            with open(file) as f:
                src = json.load(f)
        except json.JSONDecodeError as e:
            errors.append(f"{file}: invalid JSON ({e})")
            continue

        for key, typ in REQUIRED.items():
            if not isinstance(src.get(key), typ):
                errors.append(f"{file}: '{key}' missing or not {typ.__name__}")
        if errors and errors[-1].startswith(file):
            continue

        if os.path.splitext(os.path.basename(file))[0] != src["source_id"]:
            errors.append(f"{file}: file name must equal source_id")
        for key in ("source_id", "target_table", "pipeline_group"):
            if not IDENTIFIER.match(src[key]):
                errors.append(f"{file}: '{key}' must be lowercase snake_case")
        if src["format"] not in ALLOWED_FORMATS:
            errors.append(f"{file}: format '{src['format']}' not allowed")
        if src["source_id"] in seen_ids:
            errors.append(f"{file}: duplicate source_id")
        if src["target_table"] in seen_targets:
            errors.append(f"{file}: duplicate target_table")
        seen_ids.add(src["source_id"])
        seen_targets.add(src["target_table"])

        src["options"] = {k: str(v) for k, v in src["options"].items()}
        src["config_hash"] = hashlib.sha256(
            json.dumps(src, sort_keys=True).encode()
        ).hexdigest()
        sources.append(src)

    if errors:
        sys.exit("Config validation failed:\n  " + "\n  ".join(errors))
    return sources


def update_control_table(spark, sources, control_table, updated_by):
    """MERGE configs into the control table. Removed files are disabled, never deleted."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {control_table} (
          source_id STRING NOT NULL,
          target_table STRING NOT NULL,
          pipeline_group STRING NOT NULL,
          owner STRING,
          description STRING,
          path STRING NOT NULL,
          format STRING NOT NULL,
          options MAP<STRING, STRING>,
          expectations MAP<STRING, STRING>,
          enabled BOOLEAN NOT NULL,
          is_deleted BOOLEAN NOT NULL,
          config_hash STRING,
          updated_at TIMESTAMP,
          updated_by STRING
        )
        COMMENT 'Bronze ingestion control table. Updated from Git; do not edit except the enabled flag in emergencies.'
    """)

    staged = spark.createDataFrame(
        [{k: s[k] for k in list(REQUIRED) + ["config_hash"]} for s in sources],
        schema="""source_id STRING, target_table STRING, pipeline_group STRING, owner STRING,
                  description STRING, path STRING, format STRING,
                  options MAP<STRING, STRING>, expectations MAP<STRING, STRING>,
                  enabled BOOLEAN, config_hash STRING""",
    )
    staged.createOrReplaceTempView("staged_sources")

    # Only rows whose config changed are touched, so updated_at/updated_by and
    # DESCRIBE HISTORY show real changes only.
    spark.sql(f"""
        MERGE INTO {control_table} AS t
        USING staged_sources AS s
          ON t.source_id = s.source_id
        WHEN MATCHED AND (t.config_hash <> s.config_hash OR t.is_deleted) THEN UPDATE SET
          t.target_table = s.target_table, t.pipeline_group = s.pipeline_group,
          t.owner = s.owner, t.description = s.description, t.path = s.path,
          t.format = s.format, t.options = s.options, t.expectations = s.expectations,
          t.enabled = s.enabled, t.is_deleted = false, t.config_hash = s.config_hash,
          t.updated_at = current_timestamp(), t.updated_by = '{updated_by}'
        WHEN NOT MATCHED THEN INSERT (
          source_id, target_table, pipeline_group, owner, description, path, format,
          options, expectations, enabled, is_deleted, config_hash, updated_at, updated_by
        ) VALUES (
          s.source_id, s.target_table, s.pipeline_group, s.owner, s.description, s.path, s.format,
          s.options, s.expectations, s.enabled, false, s.config_hash, current_timestamp(), '{updated_by}'
        )
        WHEN NOT MATCHED BY SOURCE AND NOT t.is_deleted THEN UPDATE SET
          t.enabled = false, t.is_deleted = true,
          t.updated_at = current_timestamp(), t.updated_by = '{updated_by}'
    """)


def create_landing_folders(sources, raw_root):
    """Create <raw_root>/<pipeline_group>/<path> for every enabled source (idempotent).

    Unity Catalog volumes are mounted as /Volumes/... on Databricks compute, so
    plain os.makedirs works; the volume itself must already exist.
    """
    for src in sources:
        if src["enabled"]:
            folder = f"{raw_root.rstrip('/')}/{src['pipeline_group']}/{src['path']}"
            os.makedirs(folder, exist_ok=True)
            print(f"Landing folder ready: {folder}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-dir", required=True)
    parser.add_argument("--control-table")
    parser.add_argument("--raw-root", help="e.g. /Volumes/<catalog>/landing")
    parser.add_argument("--updated-by", default="manual")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()

    config_dir = resolve_config_dir(args.config_dir)
    print(f"Job started in folder: {os.getcwd()}")
    print(f"Reading config files from: {config_dir}")
    sources = load_and_validate(config_dir)
    print(f"{len(sources)} source configs valid")
    if args.validate_only:
        return
    if not args.control_table:
        sys.exit("--control-table is required unless --validate-only")
    if not re.match(r"^[\w.@:/-]+$", args.updated_by):
        sys.exit("--updated-by contains unsupported characters")

    from pyspark.sql import SparkSession

    update_control_table(SparkSession.builder.getOrCreate(), sources, args.control_table, args.updated_by)
    print(f"Updated {args.control_table}")
    if args.raw_root:
        create_landing_folders(sources, args.raw_root)


if __name__ == "__main__":
    main()
