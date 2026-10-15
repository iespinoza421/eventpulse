# Databricks notebook source

from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.eventpulse_bronze.events_raw"
SILVER_TABLE = "workspace.eventpulse_silver.events_clean"
REJECTED_TABLE = "workspace.eventpulse_audit.rejected_events"

required_tables = [
    BRONZE_TABLE,
    SILVER_TABLE,
    REJECTED_TABLE
]

for table in required_tables:
    if not spark.catalog.tableExists(table):
        raise ValueError(f"Required table not found: {table}")

bronze_df = spark.table(BRONZE_TABLE)
silver_df = spark.table(SILVER_TABLE)

bronze_count = bronze_df.count()
silver_count = silver_df.count()
silver_unique = silver_df.select("event_id").distinct().count()

bronze_files = bronze_df.select("source_file").distinct().count()
silver_files = silver_df.select("source_file").distinct().count()

silver_duplicates = silver_count - silver_unique

print("EVENTPULSE — SILVER PIPELINE BASELINE")
print("=" * 60)
print(f"Bronze records: {bronze_count:,}")
print(f"Silver records: {silver_count:,}")
print(f"Unique Silver events: {silver_unique:,}")
print(f"Bronze source files: {bronze_files}")
print(f"Silver source files: {silver_files}")
print(f"Duplicate Silver IDs: {silver_duplicates}")

assert silver_duplicates == 0, "Silver contains duplicate event IDs"

print("Silver baseline validation: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

bronze_ids_df = (
    spark.table(BRONZE_TABLE)
    .select(
        F.expr(
            "try_cast(NULLIF(TRIM(GLOBALEVENTID), '') AS BIGINT)"
        ).alias("event_id"),
        "source_file"
    )
)

silver_ids_df = (
    spark.table(SILVER_TABLE)
    .select("event_id")
    .distinct()
)

pending_events_df = (
    bronze_ids_df
    .filter(F.col("event_id").isNotNull())
    .join(silver_ids_df, "event_id", "left_anti")
)

invalid_id_count = bronze_ids_df.filter(
    F.col("event_id").isNull()
).count()

pending_count = pending_events_df.count()

print("EVENTPULSE — SILVER INCREMENTAL DISCOVERY")
print("=" * 60)
print(f"Bronze records: {bronze_count:,}")
print(f"Silver records: {silver_count:,}")
print(f"New event occurrences: {pending_count:,}")
print(f"Invalid Bronze event IDs: {invalid_id_count:,}")

if pending_count == 0:
    print("No new event IDs found.")
    print("Incremental discovery: PASSED")
else:
    print("New events detected for Silver processing.")
    display(pending_events_df.limit(10))


# COMMAND ----------


from pyspark.sql import functions as F

bronze_df = spark.table(BRONZE_TABLE)

event_occurrences_df = (
    bronze_df
    .withColumn(
        "event_id",
        F.expr(
            "try_cast(NULLIF(TRIM(GLOBALEVENTID), '') AS BIGINT)"
        )
    )
    .filter(F.col("event_id").isNotNull())
    .groupBy("event_id")
    .agg(
        F.count("*").alias("occurrences"),
        F.countDistinct("source_file").alias("source_files"),
        F.countDistinct("DATEADDED").alias("distinct_update_times")
    )
)

repeated_events_df = event_occurrences_df.filter(
    F.col("occurrences") > 1
)

repeated_count = repeated_events_df.count()

print("EVENTPULSE — EVENT VERSION PROFILE")
print("=" * 60)
print(f"Bronze records: {bronze_df.count():,}")
print(f"Unique event IDs: {event_occurrences_df.count():,}")
print(f"Repeated event IDs: {repeated_count:,}")

if repeated_count > 0:
    display(
        repeated_events_df.orderBy(
            F.desc("occurrences")
        ).limit(10)
    )
else:
    print("No repeated event IDs in current Bronze data.")

print("Event version profiling: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

bronze_df = spark.table(BRONZE_TABLE)
silver_df = spark.table(SILVER_TABLE)

metadata_columns = {
    "source_file",
    "ingestion_timestamp",
    "batch_id"
}

gdelt_columns = sorted(
    column
    for column in bronze_df.columns
    if column not in metadata_columns
)

if len(gdelt_columns) != 61:
    raise ValueError("Expected 61 original GDELT columns")

def with_content_hash(df):
    return df.withColumn(
        "content_hash",
        F.sha2(
            F.to_json(
                F.struct(
                    *[F.col(column) for column in gdelt_columns]
                ),
                options={"ignoreNullFields": "false"}
            ),
            256
        )
    )

bronze_hashed_df = (
    with_content_hash(bronze_df)
    .withColumn(
        "event_id",
        F.expr(
            "try_cast(NULLIF(TRIM(GLOBALEVENTID), '') AS BIGINT)"
        )
    )
)

silver_hashed_df = with_content_hash(silver_df)

comparison_df = (
    bronze_hashed_df.alias("b")
    .join(
        silver_hashed_df.select(
            "event_id",
            F.col("content_hash").alias("silver_hash")
        ).alias("s"),
        "event_id",
        "left"
    )
    .withColumn(
        "change_status",
        F.when(
            F.col("event_id").isNull(),
            "INVALID_ID"
        )
        .when(
            F.col("silver_hash").isNull(),
            "NEW"
        )
        .when(
            F.col("content_hash") != F.col("silver_hash"),
            "CHANGED"
        )
        .otherwise("UNCHANGED")
    )
)

status_df = (
    comparison_df
    .groupBy("change_status")
    .count()
    .orderBy("change_status")
)

print("EVENTPULSE — CONTENT CHANGE DETECTION")
print("=" * 60)

display(status_df)

total_compared = comparison_df.count()

if total_compared != bronze_df.count():
    raise ValueError("Content comparison changed row counts")

print(f"Total records compared: {total_compared:,}")
print("Content change detection: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

silver_versions_df = (
    spark.table(SILVER_TABLE)
    .select(
        "event_id",
        F.col("date_added").alias("silver_date_added")
    )
)

pending_changes_df = (
    comparison_df
    .filter(F.col("change_status").isin("NEW", "CHANGED"))
    .join(silver_versions_df, "event_id", "left")
    .withColumn(
        "bronze_date_added",
        F.try_to_timestamp(
            F.col("DATEADDED"),
            F.lit("yyyyMMddHHmmss")
        )
    )
    .filter(
        F.col("bronze_date_added").isNull()
        | F.col("silver_date_added").isNull()
        | (F.col("bronze_date_added") > F.col("silver_date_added"))
    )
)

invalid_ids_df = comparison_df.filter(
    F.col("change_status") == "INVALID_ID"
)

unchanged_df = comparison_df.filter(
    F.col("change_status") == "UNCHANGED"
)

new_count = pending_changes_df.filter(
    F.col("change_status") == "NEW"
).count()

changed_count = pending_changes_df.filter(
    F.col("change_status") == "CHANGED"
).count()

invalid_count = invalid_ids_df.count()
unchanged_count = unchanged_df.count()

skipped_versions = comparison_df.filter(
    F.col("change_status").isin("NEW", "CHANGED")
).count() - new_count - changed_count

total_classified = (
    new_count
    + changed_count
    + invalid_count
    + unchanged_count
    + skipped_versions
)

bronze_count = spark.table(BRONZE_TABLE).count()

print("EVENTPULSE — SILVER PROCESSING PLAN")
print("=" * 60)
print(f"New records: {new_count:,}")
print(f"Newer versions: {changed_count:,}")
print(f"Invalid IDs: {invalid_count:,}")
print(f"Unchanged records: {unchanged_count:,}")
print(f"Skipped older or equal versions: {skipped_versions:,}")
print(f"Total classified: {total_classified:,}")

if total_classified != bronze_count:
    raise ValueError("Silver change classification mismatch")

print("Silver processing plan: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

if new_count == 0 and changed_count == 0:
    validated_pending_df = None
    valid_pending_df = None
    rejected_pending_df = None

    print("EVENTPULSE — SILVER TRANSFORMATION")
    print("=" * 60)
    print("No new or changed events.")
    print("Transformation skipped safely.")

else:
    source_columns = spark.table(BRONZE_TABLE).columns

    pending_source_df = pending_changes_df.select(
        *[F.col(f"`{column}`") for column in source_columns]
    )

    typed_df = (
        pending_source_df
        .withColumn(
            "event_id",
            F.expr("try_cast(GLOBALEVENTID AS BIGINT)")
        )
        .withColumn(
    "event_date",
    F.expr(
        "try_cast(try_to_timestamp(SQLDATE, 'yyyyMMdd') AS DATE)"
    )
        )
        .withColumn(
            "date_added",
            F.try_to_timestamp(
                F.col("DATEADDED"),
                F.lit("yyyyMMddHHmmss")
            )
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
            ).otherwise(F.trim(F.col("ActionGeo_CountryCode")))
        )
        .withColumn(
            "action_latitude",
            F.expr(
                "try_cast(NULLIF(TRIM(ActionGeo_Lat), '') AS DOUBLE)"
            )
        )
        .withColumn(
            "action_longitude",
            F.expr(
                "try_cast(NULLIF(TRIM(ActionGeo_Long), '') AS DOUBLE)"
            )
        )
    )

    validated_pending_df = (
        typed_df
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

    valid_pending_df = validated_pending_df.filter(
        F.col("record_status") == "VALID"
    )

    rejected_pending_df = validated_pending_df.filter(
        F.col("record_status") == "REJECTED"
    )

    valid_count = valid_pending_df.count()
    rejected_count = rejected_pending_df.count()

    print("EVENTPULSE — SILVER TRANSFORMATION")
    print("=" * 60)
    print(f"Valid pending records: {valid_count:,}")
    print(f"Rejected pending records: {rejected_count:,}")

    assert valid_count + rejected_count == new_count + changed_count

    print("Silver transformation: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.window import Window

if valid_pending_df is None:
    deduplicated_pending_df = None
    duplicates_removed = 0

    print("EVENTPULSE — SILVER DEDUPLICATION")
    print("=" * 60)
    print("No valid pending events.")
    print("Deduplication skipped safely.")

else:
    source_columns = sorted(valid_pending_df.columns)

    ranked_df = valid_pending_df.withColumn(
        "_row_fingerprint",
        F.sha2(
            F.to_json(
                F.struct(
                    *[F.col(c) for c in source_columns]
                ),
                options={"ignoreNullFields": "false"}
            ),
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

    deduplicated_pending_df = (
        ranked_df
        .withColumn(
            "_row_number",
            F.row_number().over(dedup_window)
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number", "_row_fingerprint")
    )

    original_count = valid_pending_df.count()
    unique_count = deduplicated_pending_df.count()
    duplicates_removed = original_count - unique_count

    if (
        unique_count
        != deduplicated_pending_df
            .select("event_id")
            .distinct()
            .count()
    ):
        raise ValueError("Duplicate event IDs remain after deduplication")

    print("EVENTPULSE — SILVER DEDUPLICATION")
    print("=" * 60)
    print(f"Valid pending records: {original_count:,}")
    print(f"Unique pending events: {unique_count:,}")
    print(f"Duplicate occurrences removed: {duplicates_removed:,}")
    print("Silver deduplication: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F
from delta.tables import DeltaTable

SILVER_TABLE = "workspace.eventpulse_silver.events_clean"

print("EVENTPULSE — SILVER MERGE PREPARATION")
print("=" * 60)

if deduplicated_pending_df is None:
    merge_source_df = None

    print("No valid pending events.")
    print("MERGE preparation skipped safely.")

else:
    target_df = spark.table(SILVER_TABLE)

    target_columns = target_df.columns

    missing_columns = [
        column for column in target_columns
        if column not in deduplicated_pending_df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing source columns: {missing_columns}"
        )

    merge_source_df = deduplicated_pending_df.select(
        *[
            F.col(column)
            .cast(target_df.schema[column].dataType)
            .alias(column)
            for column in target_columns
        ]
    )

    source_count = merge_source_df.count()
    unique_count = (
        merge_source_df
        .select("event_id")
        .distinct()
        .count()
    )

    if source_count != unique_count:
        raise ValueError("MERGE source contains duplicate event IDs")

    print(f"MERGE source records: {source_count:,}")
    print(f"Unique source event IDs: {unique_count:,}")
    print("MERGE preparation: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

print("EVENTPULSE — SILVER VERSION POLICY")
print("=" * 60)

if merge_source_df is None:
    merge_decisions_df = None

    print("No pending events.")
    print("Version validation skipped safely.")

else:
    target_versions_df = (
        spark.table(SILVER_TABLE)
        .select(
            "event_id",
            F.col("date_added").alias("target_date_added")
        )
    )

    merge_decisions_df = (
        merge_source_df.alias("source")
        .join(
            target_versions_df.alias("target"),
            "event_id",
            "left"
        )
        .withColumn(
            "merge_action",
            F.when(
                F.col("target_date_added").isNull(),
                "INSERT"
            )
            .when(
                F.col("date_added") > F.col("target_date_added"),
                "UPDATE"
            )
            .when(
                F.col("date_added") < F.col("target_date_added"),
                "SKIP_OLDER"
            )
            .otherwise("SKIP_SAME_TIMESTAMP")
        )
    )

    print("MERGE DECISION SUMMARY")

    display(
        merge_decisions_df
        .groupBy("merge_action")
        .count()
        .orderBy("merge_action")
    )

    print("Silver version policy: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

STAGING_TABLE = "workspace.eventpulse_audit.silver_merge_staging"

if not spark.catalog.tableExists(STAGING_TABLE):
    raise ValueError("Silver staging table does not exist")

if merge_source_df is None:
    eligible_count = 0
    eligible_staging_df = None
else:
    current_versions_df = (
        spark.table(SILVER_TABLE)
        .select(
            "event_id",
            F.col("date_added").alias("target_date_added")
        )
    )

    eligible_staging_df = (
        merge_source_df
        .join(current_versions_df, "event_id", "left")
        .filter(
            F.col("target_date_added").isNull()
            | (F.col("date_added") > F.col("target_date_added"))
        )
        .select(*merge_source_df.columns)
    )

    eligible_count = eligible_staging_df.count()

    unique_count = (
        eligible_staging_df
        .select("event_id")
        .distinct()
        .count()
    )

    if eligible_count != unique_count:
        raise ValueError("Duplicate event IDs in staging source")

if eligible_count > 0:
    (
        eligible_staging_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(STAGING_TABLE)
    )
else:
    spark.sql(f"TRUNCATE TABLE {STAGING_TABLE}")

staging_count = spark.table(STAGING_TABLE).count()

print("EVENTPULSE — SILVER STAGING")
print("=" * 60)
print(f"Eligible events: {eligible_count}")
print(f"Staging records: {staging_count}")

if staging_count != eligible_count:
    raise ValueError("Silver staging record count mismatch")

print("Silver staging preparation: PASSED")


# COMMAND ----------


from delta.tables import DeltaTable
from pyspark.sql import functions as F

SILVER_TABLE = "workspace.eventpulse_silver.events_clean"
STAGING_TABLE = "workspace.eventpulse_audit.silver_merge_staging"

print("EVENTPULSE — INCREMENTAL SILVER MERGE")
print("=" * 60)

records_before = spark.table(SILVER_TABLE).count()

staging_df = spark.table(STAGING_TABLE)
staging_count = staging_df.count()

unique_staging_count = staging_df.select("event_id").distinct().count()

if staging_count != unique_staging_count:
    raise ValueError("Duplicate event IDs in staging")

if staging_count == 0:
    print("No eligible events.")
    print("Delta MERGE skipped safely.")
else:
    (
        DeltaTable.forName(spark, SILVER_TABLE)
        .alias("target")
        .merge(
            staging_df.alias("source"),
            "target.event_id = source.event_id"
        )
        .whenMatchedUpdateAll(
            condition="source.date_added > target.date_added"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"Staged events processed: {staging_count:,}")
    print("Delta MERGE executed.")

records_after = spark.table(SILVER_TABLE).count()

unique_after = (
    spark.table(SILVER_TABLE)
    .select("event_id")
    .distinct()
    .count()
)

print(f"Silver records before: {records_before:,}")
print(f"Silver records after: {records_after:,}")
print(f"Unique Silver events: {unique_after:,}")

if records_after != unique_after:
    raise ValueError("Duplicate event IDs detected after MERGE")

if staging_count == 0 and records_before != records_after:
    raise ValueError("Silver changed during a no-op execution")

print("Incremental Silver MERGE: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

REJECTED_TABLE = "workspace.eventpulse_audit.rejected_events"

rejected_df = spark.table(REJECTED_TABLE)

required_columns = [
    "GLOBALEVENTID",
    "source_file",
    "batch_id",
    "ingestion_timestamp",
    "record_status",
    "rejection_reason"
]

missing_columns = [
    column
    for column in required_columns
    if column not in rejected_df.columns
]

if missing_columns:
    raise ValueError(
        f"Missing rejected-event columns: {missing_columns}"
    )

total_rejected = rejected_df.count()

print("EVENTPULSE — REJECTED EVENTS BASELINE")
print("=" * 60)
print(f"Rejected records stored: {total_rejected:,}")
print(f"Total columns: {len(rejected_df.columns)}")
print(f"Required columns present: {len(required_columns)}")

print("\nREJECTION AUDIT SCHEMA")
rejected_df.select(*required_columns).printSchema()

print("Rejected events baseline: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

bronze_quality_df = (
    spark.table(BRONZE_TABLE)
    .withColumn(
        "checked_event_id",
        F.expr(
            "try_cast(NULLIF(TRIM(GLOBALEVENTID), '') AS BIGINT)"
        )
    )
    .withColumn(
        "checked_event_date",
        F.expr(
            "try_cast(try_to_timestamp(SQLDATE, 'yyyyMMdd') AS DATE)"
        )
    )
    .withColumn(
        "checked_date_added",
        F.try_to_timestamp(
            F.col("DATEADDED"),
            F.lit("yyyyMMddHHmmss")
        )
    )
    .withColumn(
        "rejection_reason_check",
        F.concat_ws(
            "; ",
            F.when(
                F.col("checked_event_id").isNull(),
                "Invalid event ID"
            ),
            F.when(
                F.col("checked_event_date").isNull(),
                "Invalid event date"
            ),
            F.when(
                F.col("checked_date_added").isNull(),
                "Invalid date added"
            ),
            F.when(
                F.col("EventCode").isNull()
                | (F.trim(F.col("EventCode")) == ""),
                "Missing event code"
            )
        )
    )
)

invalid_bronze_df = bronze_quality_df.filter(
    F.col("rejection_reason_check") != ""
)

invalid_count = invalid_bronze_df.count()
bronze_count = bronze_quality_df.count()

print("EVENTPULSE — BRONZE QUALITY REJECTION SCAN")
print("=" * 60)
print(f"Bronze records scanned: {bronze_count:,}")
print(f"Invalid records detected: {invalid_count:,}")

if invalid_count > 0:
    display(
        invalid_bronze_df.select(
            "GLOBALEVENTID",
            "source_file",
            "rejection_reason_check"
        ).limit(20)
    )
else:
    print("No invalid Bronze records detected.")

print("Bronze rejection scan: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.eventpulse_bronze.events_raw"
REJECTED_TABLE = "workspace.eventpulse_audit.rejected_events"

bronze_df = spark.table(BRONZE_TABLE)
rejected_df = spark.table(REJECTED_TABLE)

metadata_columns = {
    "source_file",
    "ingestion_timestamp",
    "batch_id"
}

gdelt_columns = sorted(
    column
    for column in bronze_df.columns
    if column not in metadata_columns
)

if len(gdelt_columns) != 61:
    raise ValueError("Expected 61 GDELT source columns")

def add_rejection_hash(df):
    return df.withColumn(
        "rejection_hash",
        F.sha2(
            F.to_json(
                F.struct(
                    F.col("source_file"),
                    *[F.col(c) for c in gdelt_columns]
                ),
                options={"ignoreNullFields": "false"}
            ),
            256
        )
    )

invalid_hashed_df = add_rejection_hash(invalid_bronze_df)

stored_hashed_df = add_rejection_hash(rejected_df)

existing_hashes_df = (
    stored_hashed_df
    .select("rejection_hash")
    .distinct()
)

new_rejections_df = (
    invalid_hashed_df
    .join(
        existing_hashes_df,
        "rejection_hash",
        "left_anti"
    )
)

new_rejections_count = new_rejections_df.count()

print("EVENTPULSE — REJECTION DEDUPLICATION")
print("=" * 60)
print(f"Invalid Bronze records: {invalid_count:,}")
print(f"New rejection candidates: {new_rejections_count:,}")

if new_rejections_count == 0:
    print("No new rejected records to store.")

print("Rejection deduplication: PASSED")


# COMMAND ----------


REJECTION_STAGING_TABLE = "workspace.eventpulse_audit.rejection_merge_staging"
REJECTED_TABLE = "workspace.eventpulse_audit.rejected_events"

if not spark.catalog.tableExists(REJECTION_STAGING_TABLE):
    (
        spark.table(REJECTED_TABLE)
        .limit(0)
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(REJECTION_STAGING_TABLE)
    )

staging_df = spark.table(REJECTION_STAGING_TABLE)
target_df = spark.table(REJECTED_TABLE)

schema_compatible = [
    (field.name, field.dataType)
    for field in staging_df.schema
] == [
    (field.name, field.dataType)
    for field in target_df.schema
]

print("EVENTPULSE — REJECTION STAGING SETUP")
print("=" * 60)
print(f"Staging table exists: {spark.catalog.tableExists(REJECTION_STAGING_TABLE)}")
print(f"Staging records: {staging_df.count()}")
print(f"Schema compatible: {schema_compatible}")

if not schema_compatible:
    raise ValueError("Rejection staging schema mismatch")

print("Rejection staging setup: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

REJECTED_TABLE = "workspace.eventpulse_audit.rejected_events"
REJECTION_STAGING_TABLE = "workspace.eventpulse_audit.rejection_merge_staging"

target_schema = spark.table(REJECTED_TABLE).schema

if new_rejections_count > 0:
    prepared_rejections_df = (
        new_rejections_df
        .withColumn("event_id", F.col("checked_event_id"))
        .withColumn("event_date", F.col("checked_event_date"))
        .withColumn("date_added", F.col("checked_date_added"))
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
            ).otherwise(F.trim(F.col("ActionGeo_CountryCode")))
        )
        .withColumn(
            "action_latitude",
            F.expr("try_cast(NULLIF(TRIM(ActionGeo_Lat), '') AS DOUBLE)")
        )
        .withColumn(
            "action_longitude",
            F.expr("try_cast(NULLIF(TRIM(ActionGeo_Long), '') AS DOUBLE)")
        )
        .withColumn("rejection_reason", F.col("rejection_reason_check"))
        .withColumn("record_status", F.lit("REJECTED"))
        .select(
            *[
                F.col(field.name).cast(field.dataType).alias(field.name)
                for field in target_schema
            ]
        )
    )

    prepared_rejections_df = (
        add_rejection_hash(prepared_rejections_df)
        .dropDuplicates(["rejection_hash"])
        .drop("rejection_hash")
    )

    (
        prepared_rejections_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(REJECTION_STAGING_TABLE)
    )
else:
    spark.sql(f"TRUNCATE TABLE {REJECTION_STAGING_TABLE}")

staging_df = spark.table(REJECTION_STAGING_TABLE)
staging_count = staging_df.count()

print("EVENTPULSE — REJECTION STAGING")
print("=" * 60)
print(f"New rejection candidates: {new_rejections_count}")
print(f"Staging records: {staging_count}")

if staging_count != new_rejections_count and new_rejections_count == 0:
    raise ValueError("Rejection staging should be empty")

print("Rejection staging preparation: PASSED")


# COMMAND ----------


from delta.tables import DeltaTable
from pyspark.sql import functions as F

REJECTED_TABLE = "workspace.eventpulse_audit.rejected_events"
REJECTION_STAGING_TABLE = "workspace.eventpulse_audit.rejection_merge_staging"

print("EVENTPULSE — REJECTED EVENTS STORAGE")
print("=" * 60)

target_df = spark.table(REJECTED_TABLE)
staging_df = spark.table(REJECTION_STAGING_TABLE)

records_before = target_df.count()
staging_count = staging_df.count()

staging_hashed_df = add_rejection_hash(staging_df)

unique_staging_count = (
    staging_hashed_df
    .select("rejection_hash")
    .distinct()
    .count()
)

if staging_count != unique_staging_count:
    raise ValueError("Duplicate rejection hashes detected in staging")

if staging_count == 0:
    print("No new rejected records.")
    print("Delta MERGE skipped safely.")
else:
    hash_fields = ["source_file", *gdelt_columns]

    target_hash_expression = (
        "sha2(to_json(named_struct("
        + ", ".join(
            f"'{column}', target.`{column}`"
            for column in hash_fields
        )
        + "), map('ignoreNullFields', 'false')), 256)"
    )

    (
        DeltaTable.forName(spark, REJECTED_TABLE)
        .alias("target")
        .merge(
            staging_hashed_df.alias("source"),
            f"{target_hash_expression} = source.rejection_hash"
        )
        .whenNotMatchedInsert(
            values={
                column: f"source.`{column}`"
                for column in target_df.columns
            }
        )
        .execute()
    )

    print(f"Staged rejection records: {staging_count}")
    print("Rejected events MERGE executed.")

records_after = spark.table(REJECTED_TABLE).count()

print(f"Rejected records before: {records_before}")
print(f"Rejected records after: {records_after}")

if records_after < records_before:
    raise ValueError("Rejected event count unexpectedly decreased")

if staging_count == 0 and records_after != records_before:
    raise ValueError("Rejected events changed during a no-op run")

print("Rejected events storage: PASSED")


# COMMAND ----------


print("EVENTPULSE — THIRD BATCH RESULTS")
print("=" * 60)

print(f"New records: {new_count}")
print(f"Newer versions: {changed_count}")
print(f"Invalid records detected: {invalid_count}")
print(f"Silver records after: {spark.table(SILVER_TABLE).count()}")
print(f"Rejected records after: {spark.table(REJECTED_TABLE).count()}")
