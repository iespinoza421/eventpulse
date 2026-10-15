# Databricks notebook source

from pyspark.sql import functions as F

VOLUME_PATH = "/Volumes/workspace/eventpulse_bronze/raw_files"

files = [
    file for file in dbutils.fs.ls(VOLUME_PATH)
    if file.name.lower().endswith(".export.csv")
]

if not files:
    raise FileNotFoundError("No GDELT Events CSV files found")

print("GDELT SOURCE FILES")
print("=" * 60)

for file in files:
    print(f"File: {file.name}")
    print(f"Size: {file.size:,} bytes")
    print(f"Path: {file.path}")



# COMMAND ----------


source_paths = [file.path for file in files]

raw_df = (
    spark.read
    .option("sep", "\t")
    .option("header", "false")
    .option("inferSchema", "false")
    .csv(source_paths)
)

print(f"Total records: {raw_df.count():,}")
print(f"Total columns: {len(raw_df.columns)}")

if len(raw_df.columns) != 61:
    raise ValueError("Unexpected GDELT Events column count")

display(raw_df.limit(5))


# COMMAND ----------


GDELT_COLUMNS = [
    "GLOBALEVENTID",
    "SQLDATE",
    "MonthYear",
    "Year",
    "FractionDate",
    "Actor1Code",
    "Actor1Name",
    "Actor1CountryCode",
    "Actor1KnownGroupCode",
    "Actor1EthnicCode",
    "Actor1Religion1Code",
    "Actor1Religion2Code",
    "Actor1Type1Code",
    "Actor1Type2Code",
    "Actor1Type3Code",
    "Actor2Code",
    "Actor2Name",
    "Actor2CountryCode",
    "Actor2KnownGroupCode",
    "Actor2EthnicCode",
    "Actor2Religion1Code",
    "Actor2Religion2Code",
    "Actor2Type1Code",
    "Actor2Type2Code",
    "Actor2Type3Code",
    "IsRootEvent",
    "EventCode",
    "EventBaseCode",
    "EventRootCode",
    "QuadClass",
    "GoldsteinScale",
    "NumMentions",
    "NumSources",
    "NumArticles",
    "AvgTone",
    "Actor1Geo_Type",
    "Actor1Geo_FullName",
    "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code",
    "Actor1Geo_ADM2Code",
    "Actor1Geo_Lat",
    "Actor1Geo_Long",
    "Actor1Geo_FeatureID",
    "Actor2Geo_Type",
    "Actor2Geo_FullName",
    "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code",
    "Actor2Geo_ADM2Code",
    "Actor2Geo_Lat",
    "Actor2Geo_Long",
    "Actor2Geo_FeatureID",
    "ActionGeo_Type",
    "ActionGeo_FullName",
    "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code",
    "ActionGeo_ADM2Code",
    "ActionGeo_Lat",
    "ActionGeo_Long",
    "ActionGeo_FeatureID",
    "DATEADDED",
    "SOURCEURL"
]

if len(GDELT_COLUMNS) != 61:
    raise ValueError("The GDELT schema must contain 61 columns")

if len(raw_df.columns) != len(GDELT_COLUMNS):
    raise ValueError("Source column count does not match GDELT schema")

named_df = raw_df.toDF(*GDELT_COLUMNS)

print(f"Records: {named_df.count():,}")
print(f"Columns: {len(named_df.columns)}")

display(
    named_df.select(
        "GLOBALEVENTID",
        "SQLDATE",
        "EventCode",
        "EventRootCode",
        "ActionGeo_CountryCode",
        "DATEADDED",
        "SOURCEURL"
    ).limit(10)
)


# COMMAND ----------


from pyspark.sql import functions as F
from datetime import datetime, timezone

batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
ingestion_time = datetime.now(timezone.utc)

bronze_df = (
    named_df
    .withColumn("source_file", F.col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", F.lit(ingestion_time))
    .withColumn("batch_id", F.lit(batch_id))
)

print(f"Batch ID: {batch_id}")
print(f"Total records: {bronze_df.count():,}")
print(f"Total columns: {len(bronze_df.columns)}")

display(
    bronze_df.select(
        "GLOBALEVENTID",
        "EventCode",
        "DATEADDED",
        "source_file",
        "ingestion_timestamp",
        "batch_id"
    ).limit(5)
)


# COMMAND ----------


from pyspark.sql import functions as F

TABLE_NAME = "workspace.eventpulse_bronze.events_raw"

source_files = [
    row.source_file
    for row in bronze_df.select("source_file").distinct().collect()
]

if spark.catalog.tableExists(TABLE_NAME):
    processed_files = {
        row.source_file
        for row in spark.table(TABLE_NAME)
        .select("source_file")
        .distinct()
        .collect()
    }
else:
    processed_files = set()

new_files = set(source_files) - processed_files

if not new_files:
    print("No new files to process. Bronze table is up to date.")
else:
    new_df = bronze_df.filter(F.col("source_file").isin(list(new_files)))

    (
        new_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(TABLE_NAME)
    )

    print(f"New files processed: {len(new_files)}")
    print(f"Records inserted: {new_df.count():,}")

print(f"Total Bronze records: {spark.table(TABLE_NAME).count():,}")


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL workspace.eventpulse_bronze.events_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_records,
# MAGIC     COUNT(DISTINCT GLOBALEVENTID) AS unique_events,
# MAGIC     COUNT(DISTINCT source_file) AS source_files,
# MAGIC     COUNT(DISTINCT batch_id) AS ingestion_batches
# MAGIC FROM workspace.eventpulse_bronze.events_raw;

# COMMAND ----------


from pyspark.sql import functions as F

VOLUME_PATH = "/Volumes/workspace/eventpulse_bronze/raw_files"
BRONZE_TABLE = "workspace.eventpulse_bronze.events_raw"

available_files = {
    file.name: file.path
    for file in dbutils.fs.ls(VOLUME_PATH)
    if file.name.lower().endswith(".export.csv")
}

processed_files = {
    row["filename"]
    for row in (
        spark.table(BRONZE_TABLE)
        .select(
            F.regexp_extract(
                F.col("source_file"),
                r"([^/]+\.export\.CSV)$",
                1
            ).alias("filename")
        )
        .distinct()
        .collect()
    )
}

pending_files = {
    name: path
    for name, path in available_files.items()
    if name not in processed_files
}

print("EVENTPULSE INGESTION STATUS")
print("=" * 60)

print(f"Available files: {len(available_files)}")
print(f"Already processed: {len(processed_files)}")
print(f"Pending files: {len(pending_files)}")

for name in sorted(pending_files):
    print(f"PENDING: {name}")


# COMMAND ----------


from pyspark.sql import functions as F

if not pending_files:
    print("No pending files to process.")
    pending_raw_df = None
else:
    pending_paths = list(pending_files.values())

    pending_raw_df = (
        spark.read
        .option("sep", "\t")
        .option("header", "false")
        .option("inferSchema", "false")
        .csv(pending_paths)
    )

    source_count = pending_raw_df.count()
    column_count = len(pending_raw_df.columns)

    print("PENDING GDELT BATCH")
    print("=" * 60)
    print(f"Pending files: {len(pending_paths)}")
    print(f"Source records: {source_count:,}")
    print(f"Total columns: {column_count}")

    if column_count != 61:
        raise ValueError(
            f"Expected 61 columns, found {column_count}"
        )

    if source_count == 0:
        raise ValueError("Pending batch contains no records")

    print("Source validation: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

VOLUME_PATH = "/Volumes/workspace/eventpulse_bronze/raw_files"

SECOND_BATCH = "20261013211500.export.CSV"

source_path = f"{VOLUME_PATH}/{SECOND_BATCH}"

pending_raw_df = (
    spark.read
    .option("sep", "\t")
    .option("header", "false")
    .option("inferSchema", "false")
    .csv(source_path)
)

source_count = pending_raw_df.count()
column_count = len(pending_raw_df.columns)

print("SECOND BATCH RECOVERY")
print("=" * 60)
print(f"Source file: {SECOND_BATCH}")
print(f"Records: {source_count:,}")
print(f"Columns: {column_count}")

if source_count != 982 or column_count != 61:
    raise ValueError("Unexpected second batch structure")

print("Recovery: PASSED")


# COMMAND ----------


from datetime import datetime, timezone
from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.eventpulse_bronze.events_raw"
VOLUME_PATH = "/Volumes/workspace/eventpulse_bronze/raw_files"
SECOND_BATCH = "20261013211500.export.CSV"

source_path = f"{VOLUME_PATH}/{SECOND_BATCH}"

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
    raise ValueError("Expected 61 original GDELT columns")

batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
ingestion_time = datetime.now(timezone.utc)

pending_source_df = (
    spark.read
    .option("sep", "\t")
    .option("header", "false")
    .option("inferSchema", "false")
    .csv(source_path)
)

if len(pending_source_df.columns) != 61:
    raise ValueError("Unexpected GDELT source schema")

pending_bronze_df = pending_source_df.select(
    *[
        F.col(f"_c{i}").alias(column)
        for i, column in enumerate(gdelt_columns)
    ],
    F.col("_metadata.file_path").alias("source_file"),
    F.lit(ingestion_time).alias("ingestion_timestamp"),
    F.lit(batch_id).alias("batch_id")
)

pending_bronze_df = pending_bronze_df.select(
    *[
        F.col(field.name).cast(field.dataType).alias(field.name)
        for field in bronze_schema
    ]
)

record_count = pending_bronze_df.count()
column_count = len(pending_bronze_df.columns)

source_files = pending_bronze_df.select(
    "source_file"
).distinct().count()

print("SECOND BATCH PREPARATION")
print("=" * 60)
print(f"Batch ID: {batch_id}")
print(f"Records: {record_count:,}")
print(f"Columns: {column_count}")
print(f"Source files: {source_files}")

if record_count != 982:
    raise ValueError("Unexpected record count")

if column_count != 64:
    raise ValueError("Unexpected Bronze column count")

if source_files != 1:
    raise ValueError("Unexpected number of source files")

print("Preparation: PASSED")

display(
    pending_bronze_df.select(
        "GLOBALEVENTID",
        "EventCode",
        "DATEADDED",
        "source_file",
        "batch_id"
    ).limit(5)
)


# COMMAND ----------


from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.eventpulse_bronze.events_raw"

source_file = pending_bronze_df.select("source_file").first()

if source_file is None:
    raise ValueError("The pending batch is empty")

source_path = source_file["source_file"]

existing_count = (
    spark.table(BRONZE_TABLE)
    .filter(F.col("source_file") == source_path)
    .count()
)

print("INCREMENTAL BRONZE INGESTION")
print("=" * 60)
print(f"Source: {source_path}")
print(f"Previously stored records: {existing_count:,}")

if existing_count > 0:
    print("File already processed. Ingestion skipped.")
else:
    (
        pending_bronze_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(BRONZE_TABLE)
    )

    print("New batch inserted successfully.")

bronze_df = spark.table(BRONZE_TABLE)

total_records = bronze_df.count()
total_files = bronze_df.select("source_file").distinct().count()

print(f"Total Bronze records: {total_records:,}")
print(f"Total source files: {total_files}")

if total_records != 2155 or total_files != 2:
    raise ValueError("Unexpected Bronze ingestion totals")

print("Bronze ingestion validation: PASSED")
