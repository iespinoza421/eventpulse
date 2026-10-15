# Databricks notebook source

from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.eventpulse_bronze.events_raw"

bronze_df = spark.table(BRONZE_TABLE)

quality_profile = bronze_df.agg(
    F.count("*").alias("total_records"),
    F.countDistinct("GLOBALEVENTID").alias("unique_events"),
    F.sum(
        F.when(
            F.col("GLOBALEVENTID").isNull()
            | (F.trim(F.col("GLOBALEVENTID")) == ""),
            1
        ).otherwise(0)
    ).alias("missing_event_ids"),
    F.sum(
        F.when(
            F.to_date("SQLDATE", "yyyyMMdd").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_event_dates"),
    F.sum(
        F.when(
            F.to_timestamp("DATEADDED", "yyyyMMddHHmmss").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_date_added"),
    F.sum(
        F.when(
            F.col("EventCode").isNull()
            | (F.trim(F.col("EventCode")) == ""),
            1
        ).otherwise(0)
    ).alias("missing_event_codes")
)

display(quality_profile)


# COMMAND ----------


from pyspark.sql import functions as F

numeric_fields = [
    "GoldsteinScale",
    "NumMentions",
    "NumSources",
    "NumArticles",
    "AvgTone",
]

expressions = []

for field in numeric_fields:
    value = F.trim(F.col(field))

    expressions.append(
        F.sum(
            F.when(
                value.isNotNull()
                & (value != "")
                & F.expr(f"try_cast(`{field}` AS DOUBLE)").isNull(),
                1
            ).otherwise(0)
        ).alias(f"invalid_{field}")
    )

expressions.extend([
    F.sum(
        F.when(
            F.col("ActionGeo_CountryCode").isNull()
            | (F.trim(F.col("ActionGeo_CountryCode")) == ""),
            1
        ).otherwise(0)
    ).alias("missing_action_country"),
    F.sum(
        F.when(
            F.col("ActionGeo_Lat").isNull()
            | (F.trim(F.col("ActionGeo_Lat")) == ""),
            1
        ).otherwise(0)
    ).alias("missing_latitude"),
    F.sum(
        F.when(
            F.col("ActionGeo_Long").isNull()
            | (F.trim(F.col("ActionGeo_Long")) == ""),
            1
        ).otherwise(0)
    ).alias("missing_longitude"),
])

display(bronze_df.agg(*expressions))


# COMMAND ----------


from pyspark.sql import functions as F

silver_typed_df = (
    bronze_df
    .withColumn(
        "event_id",
        F.expr("try_cast(GLOBALEVENTID AS BIGINT)")
    )
    .withColumn(
        "event_date",
        F.to_date("SQLDATE", "yyyyMMdd")
    )
    .withColumn(
        "date_added",
        F.to_timestamp("DATEADDED", "yyyyMMddHHmmss")
    )
    .withColumn(
        "goldstein_scale",
        F.expr("try_cast(GoldsteinScale AS DOUBLE)")
    )
    .withColumn(
        "num_mentions",
        F.expr("try_cast(NumMentions AS BIGINT)")
    )
    .withColumn(
        "num_sources",
        F.expr("try_cast(NumSources AS BIGINT)")
    )
    .withColumn(
        "num_articles",
        F.expr("try_cast(NumArticles AS BIGINT)")
    )
    .withColumn(
        "avg_tone",
        F.expr("try_cast(AvgTone AS DOUBLE)")
    )
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

display(
    silver_typed_df.select(
        "event_id",
        "event_date",
        "date_added",
        "goldstein_scale",
        "num_mentions",
        "avg_tone",
        "action_country_code",
        "action_latitude",
        "action_longitude"
    ).limit(10)
)


# COMMAND ----------


from pyspark.sql import functions as F

validated_df = (
    silver_typed_df
    .withColumn(
        "rejection_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("event_id").isNull(),
                "Invalid event ID"
            ),
            F.when(
                F.col("event_date").isNull(),
                "Invalid event date"
            ),
            F.when(
                F.col("date_added").isNull(),
                "Invalid date added"
            ),
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

quality_summary = (
    validated_df
    .groupBy("record_status")
    .agg(F.count("*").alias("records"))
    .orderBy("record_status")
)

display(quality_summary)


# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.window import Window

valid_df = validated_df.filter(
    F.col("record_status") == "VALID"
)

rejected_df = validated_df.filter(
    F.col("record_status") == "REJECTED"
)

source_columns = sorted(valid_df.columns)

ranked_df = valid_df.withColumn(
    "_row_fingerprint",
    F.sha2(
        F.to_json(F.struct(*[F.col(c) for c in source_columns])),
        256
    )
)

dedup_window = (
    Window
    .partitionBy("event_id")
    .orderBy(
        F.col("date_added").desc_nulls_last(),
        F.col("ingestion_timestamp").desc_nulls_last(),
        F.col("batch_id").desc_nulls_last(),
        F.col("source_file").desc_nulls_last(),
        F.col("_row_fingerprint").desc()
    )
)

deduplicated_df = (
    ranked_df
    .withColumn("_row_number", F.row_number().over(dedup_window))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number", "_row_fingerprint")
)

total_valid = valid_df.count()
total_rejected = rejected_df.count()
total_unique = deduplicated_df.count()
duplicates_removed = total_valid - total_unique

print("SILVER DATA QUALITY SUMMARY")
print("=" * 60)
print(f"Valid records: {total_valid:,}")
print(f"Rejected records: {total_rejected:,}")
print(f"Unique events: {total_unique:,}")
print(f"Duplicate occurrences removed: {duplicates_removed:,}")

assert total_valid == total_unique + duplicates_removed


# COMMAND ----------


SILVER_TABLE = "workspace.eventpulse_silver.events_clean"

if spark.catalog.tableExists(SILVER_TABLE):
    print(f"Table already exists: {SILVER_TABLE}")
    print("Initial load skipped.")
else:
    (
        deduplicated_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(SILVER_TABLE)
    )

    print(f"Table created: {SILVER_TABLE}")

silver_saved_df = spark.table(SILVER_TABLE)

total_saved = silver_saved_df.count()
unique_saved = silver_saved_df.select("event_id").distinct().count()

if total_saved != total_unique:
    raise ValueError("Silver record count does not match")

if total_saved != unique_saved:
    raise ValueError("Duplicate event IDs found in Silver")

print("\nSILVER TABLE VALIDATION")
print("=" * 60)
print(f"Records saved: {total_saved:,}")
print(f"Unique events: {unique_saved:,}")
print("Validation: PASSED")


# COMMAND ----------


from datetime import datetime, timezone
from pyspark.sql import Row

AUDIT_TABLE = "workspace.eventpulse_audit.reconciliation"

bronze_count = bronze_df.count()
silver_count = spark.table(
    "workspace.eventpulse_silver.events_clean"
).count()

rejected_count = rejected_df.count()
duplicate_count = duplicates_removed

reconciled = (
    bronze_count
    == silver_count + rejected_count + duplicate_count
)

audit_record = Row(
    execution_timestamp=datetime.now(timezone.utc),
    pipeline_name="eventpulse_bronze_to_silver",
    bronze_records=bronze_count,
    silver_records=silver_count,
    rejected_records=rejected_count,
    duplicate_records=duplicate_count,
    reconciliation_passed=reconciled
)

audit_df = spark.createDataFrame([audit_record])

display(audit_df)

if not reconciled:
    raise ValueError("Bronze-to-Silver reconciliation failed")


# COMMAND ----------


AUDIT_TABLE = "workspace.eventpulse_audit.reconciliation"

if not reconciled:
    raise ValueError("Cannot save a failed reconciliation")

(
    audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(AUDIT_TABLE)
)

print("AUDIT RECORD SAVED")
print("=" * 60)
print(f"Table: {AUDIT_TABLE}")

display(
    spark.table(AUDIT_TABLE)
    .orderBy("execution_timestamp", ascending=False)
    .limit(10)
)


# COMMAND ----------


REJECTED_TABLE = "workspace.eventpulse_audit.rejected_events"

rejected_count = rejected_df.count()

if spark.catalog.tableExists(REJECTED_TABLE):
    print(f"Rejected events table already exists: {REJECTED_TABLE}")
elif rejected_count == 0:
    (
        rejected_df.limit(0)
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(REJECTED_TABLE)
    )
    print("Empty rejected events table created.")
else:
    print("Rejected records are available for initial loading.")

print(f"Rejected records in current batch: {rejected_count:,}")
print(f"Total stored rejected records: {spark.table(REJECTED_TABLE).count():,}")


# COMMAND ----------


SILVER_TABLE = "workspace.eventpulse_silver.events_clean"

silver_df = spark.table(SILVER_TABLE)

required_columns = [
    "event_id",
    "event_date",
    "date_added",
    "source_file",
    "ingestion_timestamp",
    "batch_id",
    "record_status",
    "rejection_reason",
]

missing_columns = [
    column
    for column in required_columns
    if column not in silver_df.columns
]

if missing_columns:
    raise ValueError(
        f"Missing Silver columns: {missing_columns}"
    )

print("SILVER SCHEMA VALIDATION")
print("=" * 60)
print(f"Total columns: {len(silver_df.columns)}")
print(f"Total records: {silver_df.count():,}")
print(f"Unique event IDs: {silver_df.select('event_id').distinct().count():,}")
print("Required columns: PASSED")

silver_df.select(*required_columns).printSchema()
