# Databricks notebook source

from pyspark.sql import functions as F

TEST_TABLE = "workspace.eventpulse_audit.merge_validation"

source_df = (
    spark.table("workspace.eventpulse_silver.events_clean")
    .select("event_id", "date_added", "EventCode")
    .orderBy("event_id")
    .limit(5)
    .withColumn("processing_status", F.lit("ORIGINAL"))
)

if not spark.catalog.tableExists(TEST_TABLE):
    (
        source_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(TEST_TABLE)
    )

display(spark.table(TEST_TABLE))


# COMMAND ----------


from delta.tables import DeltaTable
from pyspark.sql import functions as F

target = DeltaTable.forName(spark, TEST_TABLE)

original_df = spark.table(TEST_TABLE)

existing_event = (
    original_df
    .orderBy("event_id")
    .limit(1)
)

new_event_id = (
    original_df
    .agg(F.max("event_id").alias("max_id"))
    .first()["max_id"] + 1000000
)

update_row = (
    existing_event
    .withColumn("processing_status", F.lit("UPDATED"))
)

insert_row = (
    existing_event
    .withColumn("event_id", F.lit(new_event_id))
    .withColumn("processing_status", F.lit("INSERTED"))
)

merge_source = update_row.unionByName(insert_row)

(
    target.alias("target")
    .merge(
        merge_source.alias("source"),
        "target.event_id = source.event_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("MERGE EXECUTED")
print("=" * 60)

display(
    spark.table(TEST_TABLE)
    .orderBy("event_id")
)


# COMMAND ----------


from delta.tables import DeltaTable
from pyspark.sql import functions as F

target = DeltaTable.forName(spark, TEST_TABLE)

records_before = spark.table(TEST_TABLE).count()

(
    target.alias("target")
    .merge(
        merge_source.alias("source"),
        "target.event_id = source.event_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

result_df = spark.table(TEST_TABLE)

records_after = result_df.count()
unique_events = result_df.select("event_id").distinct().count()

status_counts = {
    row["processing_status"]: row["count"]
    for row in result_df.groupBy("processing_status").count().collect()
}

assert records_before == 6
assert records_after == records_before
assert unique_events == records_after

assert status_counts == {
    "ORIGINAL": 4,
    "UPDATED": 1,
    "INSERTED": 1,
}

print("MERGE IDEMPOTENCY VALIDATION")
print("=" * 60)
print(f"Records before: {records_before}")
print(f"Records after: {records_after}")
print(f"Unique events: {unique_events}")
print("Validation: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.eventpulse_bronze.events_raw"
SILVER_TABLE = "workspace.eventpulse_silver.events_clean"

bronze_df = spark.table(BRONZE_TABLE)
silver_df = spark.table(SILVER_TABLE)

bronze_columns = set(bronze_df.columns)
silver_columns = set(silver_df.columns)

original_columns = bronze_columns - {
    "source_file",
    "ingestion_timestamp",
    "batch_id",
}

missing_original = original_columns - silver_columns
silver_additional = sorted(silver_columns - bronze_columns)

if missing_original:
    raise ValueError(
        f"Original columns missing in Silver: {sorted(missing_original)}"
    )

print("INCREMENTAL PIPELINE SCHEMA CHECK")
print("=" * 60)
print(f"Bronze columns: {len(bronze_columns)}")
print(f"Silver columns: {len(silver_columns)}")
print(f"Original columns preserved: {len(original_columns)}")
print(f"Additional Silver columns: {len(silver_additional)}")

print("\nAdditional Silver fields:")
for column in silver_additional:
    print(f"- {column}")

print("\nSchema validation: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.eventpulse_bronze.events_raw"
SILVER_TABLE = "workspace.eventpulse_silver.events_clean"

bronze_df = spark.table(BRONZE_TABLE)
silver_df = spark.table(SILVER_TABLE)
silver_schema = silver_df.schema

typed_df = (
    bronze_df
    .withColumn("event_id", F.expr("try_cast(GLOBALEVENTID AS BIGINT)"))
    .withColumn("event_date", F.to_date("SQLDATE", "yyyyMMdd"))
    .withColumn("date_added", F.to_timestamp("DATEADDED", "yyyyMMddHHmmss"))
    .withColumn("goldstein_scale", F.expr("try_cast(GoldsteinScale AS DOUBLE)"))
    .withColumn("num_mentions", F.expr("try_cast(NumMentions AS BIGINT)"))
    .withColumn("num_sources", F.expr("try_cast(NumSources AS BIGINT)"))
    .withColumn("num_articles", F.expr("try_cast(NumArticles AS BIGINT)"))
    .withColumn("avg_tone", F.expr("try_cast(AvgTone AS DOUBLE)"))
    .withColumn(
        "action_country_code",
        F.when(
            F.trim(F.col("ActionGeo_CountryCode")) == "",
            F.lit(None)
        ).otherwise(F.col("ActionGeo_CountryCode"))
    )
    .withColumn(
        "action_latitude",
        F.expr("try_cast(NULLIF(TRIM(ActionGeo_Lat), '') AS DOUBLE)")
    )
    .withColumn(
        "action_longitude",
        F.expr("try_cast(NULLIF(TRIM(ActionGeo_Long), '') AS DOUBLE)")
    )
)

validated_df = (
    typed_df
    .withColumn(
        "rejection_reason",
        F.concat_ws(
            "; ",
            F.when(F.col("event_id").isNull(), "Invalid event ID"),
            F.when(F.col("event_date").isNull(), "Invalid event date"),
            F.when(F.col("date_added").isNull(), "Invalid date added"),
            F.when(
                F.col("EventCode").isNull()
                | (F.trim(F.col("EventCode")) == ""),
                "Missing event code"
            )
        )
    )
    .withColumn(
        "record_status",
        F.when(
            F.col("rejection_reason") == "",
            "VALID"
        ).otherwise("REJECTED")
    )
)

valid_df = validated_df.filter(
    F.col("record_status") == "VALID"
)

rejected_df = validated_df.filter(
    F.col("record_status") == "REJECTED"
)

target_fields = {
    field.name: field
    for field in silver_schema
}

missing_fields = [
    name
    for name in target_fields
    if name not in valid_df.columns
]

if missing_fields:
    raise ValueError(f"Missing Silver fields: {missing_fields}")

incremental_df = valid_df.select(
    *[
        F.col(field.name).cast(field.dataType).alias(field.name)
        for field in silver_schema
    ]
)

source_fields = {
    field.name: field
    for field in incremental_df.schema
}

names_match = set(target_fields) == set(source_fields)

types_match = all(
    source_fields[name].dataType == target.dataType
    for name, target in target_fields.items()
    if name in source_fields
)

required_fields = [
    name
    for name, field in target_fields.items()
    if not field.nullable
]

if required_fields:
    result = incremental_df.agg(
        *[
            F.sum(
                F.col(name).isNull().cast("int")
            ).alias(name)
            for name in required_fields
        ]
    ).first()

    if result is None:
        raise ValueError("Nullability validation returned no result")

    null_counts = result.asDict()

    nullability_valid = all(
        count == 0
        for count in null_counts.values()
    )

    nullability_valid = all(
        count == 0
        for count in null_counts.values()
    )
else:
    nullability_valid = True

schema_compatible = (
    names_match
    and types_match
    and nullability_valid
)

print("INCREMENTAL DATA PREPARATION")
print("=" * 60)
print(f"Source records: {bronze_df.count():,}")
print(f"Valid records: {valid_df.count():,}")
print(f"Rejected records: {rejected_df.count():,}")
print(f"Prepared records: {incremental_df.count():,}")

print("\nINCREMENTAL SCHEMA VALIDATION")
print("=" * 60)
print(f"Target columns: {len(target_fields)}")
print(f"Source columns: {len(source_fields)}")
print(f"Column names match: {names_match}")
print(f"Data types match: {types_match}")
print(f"Required fields contain no NULLs: {nullability_valid}")
print(f"Schema compatible: {schema_compatible}")

if not schema_compatible:
    raise ValueError("Incremental schema validation failed")


# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.window import Window

source_columns = sorted(incremental_df.columns)

ranked_df = incremental_df.withColumn(
    "_row_fingerprint",
    F.sha2(
        F.to_json(
            F.struct(*[F.col(name) for name in source_columns]),
            options={"ignoreNullFields": "false"}
        ),
        256
    )
)

dedup_window = (
    Window.partitionBy("event_id")
    .orderBy(
        F.col("date_added").desc_nulls_last(),
        F.col("ingestion_timestamp").desc_nulls_last(),
        F.col("batch_id").desc_nulls_last(),
        F.col("source_file").desc_nulls_last(),
        F.col("_row_fingerprint").desc()
    )
)

incremental_unique_df = (
    ranked_df
    .withColumn("_row_number", F.row_number().over(dedup_window))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number", "_row_fingerprint")
)

source_count = incremental_df.count()
unique_count = incremental_unique_df.count()
duplicates_removed = source_count - unique_count

print("INCREMENTAL DEDUPLICATION")
print("=" * 60)
print(f"Source records: {source_count:,}")
print(f"Unique events: {unique_count:,}")
print(f"Duplicate occurrences removed: {duplicates_removed:,}")

assert (
    incremental_unique_df.select("event_id").distinct().count()
    == unique_count
)

print("Deduplication validation: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

source_df = incremental_unique_df.alias("source")
target_df = spark.table(SILVER_TABLE).alias("target")

original_columns = sorted([
    name for name in bronze_df.columns
    if name not in {
        "source_file",
        "ingestion_timestamp",
        "batch_id",
    }
])

def content_hash(alias):
    return F.sha2(
        F.to_json(
            F.struct(*[
                F.col(f"{alias}.`{name}`").alias(name)
                for name in original_columns
            ]),
            options={"ignoreNullFields": "false"}
        ),
        256
    )

comparison_df = (
    source_df
    .join(
        target_df,
        F.col("source.event_id") == F.col("target.event_id"),
        "left"
    )
    .select(
        F.col("source.event_id").alias("event_id"),
        F.col("source.date_added").alias("source_date_added"),
        F.col("target.date_added").alias("target_date_added"),
        F.col("target.event_id").isNull().alias("is_new"),
        (content_hash("source") != content_hash("target")).alias("content_changed")
    )
    .withColumn(
        "merge_action",
        F.when(F.col("is_new"), "INSERT")
        .when(
            (F.col("source_date_added") > F.col("target_date_added"))
            & F.col("content_changed"),
            "UPDATE"
        )
        .otherwise("UNCHANGED")
    )
)

action_summary = (
    comparison_df
    .groupBy("merge_action")
    .count()
    .orderBy("merge_action")
)

display(action_summary)

action_counts = {
    row["merge_action"]: row["count"]
    for row in action_summary.collect()
}

assert sum(action_counts.values()) == incremental_unique_df.count()

print("Incremental comparison: PASSED")


# COMMAND ----------


from delta.tables import DeltaTable
from pyspark.sql import functions as F

SILVER_TABLE = "workspace.eventpulse_silver.events_clean"

changes_df = (
    incremental_unique_df.alias("source")
    .join(
        comparison_df.select("event_id", "merge_action").alias("actions"),
        F.col("source.event_id") == F.col("actions.event_id"),
        "inner"
    )
    .filter(F.col("actions.merge_action").isin("INSERT", "UPDATE"))
    .select("source.*")
)

changes_count = changes_df.count()

print("INCREMENTAL MERGE")
print("=" * 60)
print(f"Records requiring changes: {changes_count:,}")

if changes_count == 0:
    print("No changes detected. MERGE skipped.")
else:
    target = DeltaTable.forName(spark, SILVER_TABLE)

    (
        target.alias("target")
        .merge(
            changes_df.alias("source"),
            "target.event_id = source.event_id"
        )
        .whenMatchedUpdate(
            condition="source.date_added > target.date_added",
            set={
                column: f"source.`{column}`"
                for column in changes_df.columns
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("MERGE executed successfully.")

result_df = spark.table(SILVER_TABLE)

total_records = result_df.count()
unique_events = result_df.select("event_id").distinct().count()

if total_records != unique_events:
    raise ValueError("Duplicate event IDs detected after MERGE")

print(f"Silver records: {total_records:,}")
print(f"Unique events: {unique_events:,}")
print("MERGE validation: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.eventpulse_bronze.events_raw"
SILVER_TABLE = "workspace.eventpulse_silver.events_clean"

SECOND_BATCH = "20261013211500.export.CSV"

bronze_df = spark.table(BRONZE_TABLE)
silver_df = spark.table(SILVER_TABLE)

second_batch_df = bronze_df.filter(
    F.col("source_file").endswith(SECOND_BATCH)
)

batch_records = second_batch_df.count()
unique_ids = second_batch_df.select(
    "GLOBALEVENTID"
).distinct().count()

existing_ids = (
    second_batch_df.alias("source")
    .join(
        silver_df.alias("target"),
        F.expr(
            "try_cast(source.GLOBALEVENTID AS BIGINT)"
        ) == F.col("target.event_id"),
        "left_semi"
    )
    .select("GLOBALEVENTID")
    .distinct()
    .count()
)

new_ids = unique_ids - existing_ids

print("SECOND BATCH INCREMENTAL ANALYSIS")
print("=" * 60)
print(f"Batch records: {batch_records:,}")
print(f"Unique event IDs: {unique_ids:,}")
print(f"Existing event IDs: {existing_ids:,}")
print(f"New event IDs: {new_ids:,}")

if batch_records != 982:
    raise ValueError("Unexpected second batch record count")

if existing_ids + new_ids != unique_ids:
    raise ValueError("Event ID reconciliation failed")

print("Incremental analysis: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

SILVER_TABLE = "workspace.eventpulse_silver.events_clean"
silver_schema = spark.table(SILVER_TABLE).schema

typed_batch_df = (
    second_batch_df
    .withColumn("event_id", F.expr("try_cast(GLOBALEVENTID AS BIGINT)"))
    .withColumn("event_date", F.to_date("SQLDATE", "yyyyMMdd"))
    .withColumn("date_added", F.to_timestamp("DATEADDED", "yyyyMMddHHmmss"))
    .withColumn("goldstein_scale", F.expr("try_cast(GoldsteinScale AS DOUBLE)"))
    .withColumn("num_mentions", F.expr("try_cast(NumMentions AS BIGINT)"))
    .withColumn("num_sources", F.expr("try_cast(NumSources AS BIGINT)"))
    .withColumn("num_articles", F.expr("try_cast(NumArticles AS BIGINT)"))
    .withColumn("avg_tone", F.expr("try_cast(AvgTone AS DOUBLE)"))
    .withColumn(
        "action_country_code",
        F.when(
            F.trim(F.col("ActionGeo_CountryCode")) == "",
            F.lit(None)
        ).otherwise(F.col("ActionGeo_CountryCode"))
    )
    .withColumn(
        "action_latitude",
        F.expr("try_cast(NULLIF(TRIM(ActionGeo_Lat), '') AS DOUBLE)")
    )
    .withColumn(
        "action_longitude",
        F.expr("try_cast(NULLIF(TRIM(ActionGeo_Long), '') AS DOUBLE)")
    )
)

validated_batch_df = (
    typed_batch_df
    .withColumn(
        "rejection_reason",
        F.concat_ws(
            "; ",
            F.when(F.col("event_id").isNull(), "Invalid event ID"),
            F.when(F.col("event_date").isNull(), "Invalid event date"),
            F.when(F.col("date_added").isNull(), "Invalid date added"),
            F.when(
                F.col("EventCode").isNull()
                | (F.trim(F.col("EventCode")) == ""),
                "Missing event code"
            )
        )
    )
    .withColumn(
        "record_status",
        F.when(
            F.col("rejection_reason") == "",
            "VALID"
        ).otherwise("REJECTED")
    )
)

valid_batch_df = validated_batch_df.filter(
    F.col("record_status") == "VALID"
)

rejected_batch_df = validated_batch_df.filter(
    F.col("record_status") == "REJECTED"
)

prepared_batch_df = valid_batch_df.select(
    *[
        F.col(field.name).cast(field.dataType).alias(field.name)
        for field in silver_schema
    ]
)

source_count = second_batch_df.count()
valid_count = valid_batch_df.count()
rejected_count = rejected_batch_df.count()
prepared_count = prepared_batch_df.count()
unique_count = prepared_batch_df.select("event_id").distinct().count()

print("SECOND BATCH SILVER PREPARATION")
print("=" * 60)
print(f"Source records: {source_count:,}")
print(f"Valid records: {valid_count:,}")
print(f"Rejected records: {rejected_count:,}")
print(f"Prepared records: {prepared_count:,}")
print(f"Unique event IDs: {unique_count:,}")

assert source_count == valid_count + rejected_count
assert prepared_count == unique_count

print("Silver preparation: PASSED")


# COMMAND ----------


from delta.tables import DeltaTable
from pyspark.sql import functions as F

SILVER_TABLE = "workspace.eventpulse_silver.events_clean"

source_df = prepared_batch_df

source_count = source_df.count()
source_unique = source_df.select("event_id").distinct().count()
source_null_ids = source_df.filter(F.col("event_id").isNull()).count()

if source_count != source_unique or source_null_ids != 0:
    raise ValueError("Source contains duplicate or invalid event IDs")

target_before = spark.table(SILVER_TABLE)
records_before = target_before.count()

existing_before = (
    source_df.select("event_id")
    .join(target_before.select("event_id"), "event_id", "inner")
    .count()
)

new_before = source_count - existing_before

print("SECOND BATCH MERGE")
print("=" * 60)
print(f"Source records: {source_count:,}")
print(f"Silver records before: {records_before:,}")
print(f"Existing event IDs: {existing_before:,}")
print(f"New event IDs: {new_before:,}")

if existing_before > 0:
    raise ValueError(
        "Existing event IDs detected. Review before performing the merge."
    )

target = DeltaTable.forName(spark, SILVER_TABLE)

(
    target.alias("target")
    .merge(
        source_df.alias("source"),
        "target.event_id = source.event_id"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

result_df = spark.table(SILVER_TABLE)

records_after = result_df.count()
unique_after = result_df.select("event_id").distinct().count()

if records_after != records_before + new_before:
    raise ValueError("Unexpected record count after MERGE")

if records_after != unique_after:
    raise ValueError("Duplicate event IDs detected in Silver")

print("\nMERGE RESULTS")
print("=" * 60)
print(f"Silver records after: {records_after:,}")
print(f"Unique events: {unique_after:,}")
print(f"Records added: {records_after - records_before:,}")
print("Incremental MERGE validation: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.eventpulse_bronze.events_raw"
SILVER_TABLE = "workspace.eventpulse_silver.events_clean"

bronze_df = spark.table(BRONZE_TABLE)
silver_df = spark.table(SILVER_TABLE)

bronze_ids = (
    bronze_df
    .select(
        F.expr("try_cast(GLOBALEVENTID AS BIGINT)").alias("event_id")
    )
    .filter(F.col("event_id").isNotNull())
    .distinct()
)

silver_ids = (
    silver_df
    .select("event_id")
    .distinct()
)

missing_in_silver = bronze_ids.join(
    silver_ids,
    "event_id",
    "left_anti"
).count()

unexpected_in_silver = silver_ids.join(
    bronze_ids,
    "event_id",
    "left_anti"
).count()

bronze_count = bronze_df.count()
silver_count = silver_df.count()
silver_unique_count = silver_ids.count()

reconciliation_passed = (
    bronze_count == 2155
    and silver_count == 2155
    and silver_unique_count == silver_count
    and missing_in_silver == 0
    and unexpected_in_silver == 0
)

print("EVENTPULSE RECONCILIATION")
print("=" * 60)
print(f"Bronze records: {bronze_count:,}")
print(f"Silver records: {silver_count:,}")
print(f"Unique Silver events: {silver_unique_count:,}")
print(f"Missing in Silver: {missing_in_silver:,}")
print(f"Unexpected in Silver: {unexpected_in_silver:,}")
print(f"Reconciliation passed: {reconciliation_passed}")

if not reconciliation_passed:
    raise ValueError("Bronze-to-Silver reconciliation failed")


# COMMAND ----------


from datetime import datetime, timezone
from pyspark.sql import Row

AUDIT_TABLE = "workspace.eventpulse_audit.reconciliation"

if not reconciliation_passed:
    raise ValueError("Cannot save a failed reconciliation")

previous_audit = spark.table(AUDIT_TABLE)

audit_record = Row(
    execution_timestamp=datetime.now(timezone.utc),
    pipeline_name="eventpulse_bronze_to_silver",
    bronze_records=bronze_count,
    silver_records=silver_count,
    rejected_records=0,
    duplicate_records=bronze_count - silver_unique_count,
    reconciliation_passed=reconciliation_passed
)

new_audit_df = spark.createDataFrame(
    [audit_record],
    schema=previous_audit.schema
)

(
    new_audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(AUDIT_TABLE)
)

print("EVENTPULSE AUDIT")
print("=" * 60)
print("Reconciliation record saved.")

display(
    spark.table(AUDIT_TABLE)
    .orderBy("execution_timestamp", ascending=False)
)
