# Databricks notebook source

from pyspark.sql import functions as F

VOLUME_PATH = "/Volumes/workspace/eventpulse_bronze/raw_files"
BRONZE_TABLE = "workspace.eventpulse_bronze.events_raw"

available_files = {
    file.path
    for file in dbutils.fs.ls(VOLUME_PATH)
    if file.name.lower().endswith(".export.csv")
}

if spark.catalog.tableExists(BRONZE_TABLE):
    processed_files = {
        row["source_file"]
        for row in (
            spark.table(BRONZE_TABLE)
            .select("source_file")
            .distinct()
            .collect()
        )
    }
else:
    processed_files = set()

pending_files = sorted(available_files - processed_files)

print("EVENTPULSE — BRONZE PIPELINE")
print("=" * 60)
print(f"Available GDELT files: {len(available_files)}")
print(f"Previously processed: {len(available_files & processed_files)}")
print(f"Pending files: {len(pending_files)}")

for path in pending_files:
    print(f"PENDING: {path}")

print("Bronze discovery: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

if not pending_files:
    print("EVENTPULSE — INCREMENTAL SOURCE READING")
    print("=" * 60)
    print("No pending files found.")
    print("Reading skipped safely.")

    pending_raw_df = None

else:
    pending_raw_df = (
        spark.read
        .option("sep", "\t")
        .option("header", "false")
        .option("inferSchema", "false")
        .csv(pending_files)
        .select(
            "*",
            F.col("_metadata.file_path").alias("source_file")
        )
    )

    column_count = len(pending_raw_df.columns)
    record_count = pending_raw_df.count()

    print("EVENTPULSE — INCREMENTAL SOURCE READING")
    print("=" * 60)
    print(f"Pending files: {len(pending_files)}")
    print(f"Records detected: {record_count:,}")
    print(f"Columns detected: {column_count}")

    if column_count != 62:
        raise ValueError(
            f"Expected 62 columns including metadata, found {column_count}"
        )

    if record_count == 0:
        raise ValueError("Pending source files contain no records")

    print("Incremental source reading: PASSED")


# COMMAND ----------


from datetime import datetime, timezone
from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.eventpulse_bronze.events_raw"

bronze_schema = spark.table(BRONZE_TABLE).schema

metadata_columns = {
    "source_file",
    "ingestion_timestamp",
    "batch_id"
}

gdelt_columns = [
    field.name
    for field in bronze_schema
    if field.name not in metadata_columns
]

if len(gdelt_columns) != 61:
    raise ValueError(
        f"Expected 61 GDELT columns, found {len(gdelt_columns)}"
    )

if pending_raw_df is None:
    prepared_bronze_df = None

    print("EVENTPULSE — BRONZE PREPARATION")
    print("=" * 60)
    print("No new files to prepare.")
    print("Preparation skipped safely.")

else:
    ingestion_time = datetime.now(timezone.utc)
    batch_id = ingestion_time.strftime("%Y%m%dT%H%M%SZ")

    prepared_bronze_df = pending_raw_df.select(
        *[
            F.col(f"_c{i}").alias(column)
            for i, column in enumerate(gdelt_columns)
        ],
        F.col("source_file"),
        F.lit(ingestion_time).alias("ingestion_timestamp"),
        F.lit(batch_id).alias("batch_id")
    )

    # Match the existing Bronze table schema
    prepared_bronze_df = prepared_bronze_df.select(
        *[
            F.col(field.name)
            .cast(field.dataType)
            .alias(field.name)
            for field in bronze_schema
        ]
    )

    print("EVENTPULSE — BRONZE PREPARATION")
    print("=" * 60)
    print(f"Batch ID: {batch_id}")
    print(f"Records prepared: {prepared_bronze_df.count():,}")
    print(f"Columns prepared: {len(prepared_bronze_df.columns)}")

    if len(prepared_bronze_df.columns) != 64:
        raise ValueError("Unexpected Bronze schema")

    print("Bronze preparation: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.eventpulse_bronze.events_raw"

print("EVENTPULSE — INCREMENTAL BRONZE WRITE")
print("=" * 60)

records_before = spark.table(BRONZE_TABLE).count()

if prepared_bronze_df is None:
    print("No pending records.")
    print("Delta write skipped.")

else:
    # Recheck existing source files before writing
    processed_files = {
        row["source_file"]
        for row in (
            spark.table(BRONZE_TABLE)
            .select("source_file")
            .distinct()
            .collect()
        )
    }

    new_df = prepared_bronze_df.filter(
        ~F.col("source_file").isin(list(processed_files))
    )

    new_count = new_df.count()

    if new_count == 0:
        print("All source files are already processed.")
        print("Delta write skipped.")
    else:
        (
            new_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(BRONZE_TABLE)
        )

        print(f"Records inserted: {new_count:,}")

records_after = spark.table(BRONZE_TABLE).count()

print(f"Bronze records before: {records_before:,}")
print(f"Bronze records after: {records_after:,}")

if prepared_bronze_df is None and records_before != records_after:
    raise ValueError("Bronze changed during a no-op execution")

print("Incremental Bronze write: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

bronze_df = spark.table(BRONZE_TABLE)

total_records = bronze_df.count()
total_files = bronze_df.select("source_file").distinct().count()
total_batches = bronze_df.select("batch_id").distinct().count()

missing_metadata = bronze_df.filter(
    F.col("source_file").isNull()
    | F.col("batch_id").isNull()
    | F.col("ingestion_timestamp").isNull()
).count()

duplicate_file_events = (
    bronze_df
    .groupBy("source_file", "GLOBALEVENTID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

validation_passed = (
    total_records > 0
    and total_files > 0
    and total_batches > 0
    and missing_metadata == 0
)

print("EVENTPULSE — BRONZE INTEGRITY")
print("=" * 60)
print(f"Total records: {total_records:,}")
print(f"Source files: {total_files}")
print(f"Ingestion batches: {total_batches}")
print(f"Missing metadata: {missing_metadata}")
print(f"Repeated file/event IDs: {duplicate_file_events}")
print(f"Bronze integrity: {validation_passed}")

if not validation_passed:
    raise ValueError("Bronze integrity validation failed")
