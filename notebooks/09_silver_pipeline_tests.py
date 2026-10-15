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

pending_changes_df = comparison_df.filter(
    F.col("change_status").isin("NEW", "CHANGED")
)

invalid_ids_df = comparison_df.filter(
    F.col("change_status") == "INVALID_ID"
)

unchanged_df = comparison_df.filter(
    F.col("change_status") == "UNCHANGED"
)

new_count = comparison_df.filter(
    F.col("change_status") == "NEW"
).count()

changed_count = comparison_df.filter(
    F.col("change_status") == "CHANGED"
).count()

invalid_count = invalid_ids_df.count()
unchanged_count = unchanged_df.count()

total_classified = (
    new_count
    + changed_count
    + invalid_count
    + unchanged_count
)

bronze_count = spark.table(BRONZE_TABLE).count()

print("EVENTPULSE — SILVER PROCESSING PLAN")
print("=" * 60)
print(f"New records: {new_count:,}")
print(f"Changed records: {changed_count:,}")
print(f"Invalid IDs: {invalid_count:,}")
print(f"Unchanged records: {unchanged_count:,}")
print(f"Total classified: {total_classified:,}")

if total_classified != bronze_count:
    raise ValueError("Silver change classification mismatch")

if new_count == 0 and changed_count == 0:
    print("No valid content changes detected.")
    print("Silver processing can be skipped.")

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


from delta.tables import DeltaTable
from pyspark.sql import functions as F

SILVER_TABLE = "workspace.eventpulse_silver.events_clean"

print("EVENTPULSE — INCREMENTAL SILVER MERGE")
print("=" * 60)

records_before = spark.table(SILVER_TABLE).count()

if merge_source_df is None:
    print("No pending events.")
    print("Delta MERGE skipped safely.")

else:
    # Only insert new events or update newer versions
    eligible_df = (
        merge_decisions_df
        .filter(
            F.col("merge_action").isin("INSERT", "UPDATE")
        )
        .select(*merge_source_df.columns)
    )

    eligible_count = eligible_df.count()

    if eligible_count == 0:
        print("No eligible events for MERGE.")
        print("Delta MERGE skipped.")
    else:
        target = DeltaTable.forName(spark, SILVER_TABLE)

        (
            target.alias("target")
            .merge(
                eligible_df.alias("source"),
                "target.event_id = source.event_id"
            )
            .whenMatchedUpdateAll(
                condition="source.date_added > target.date_added"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"Eligible source events: {eligible_count:,}")
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

if merge_source_df is None and records_before != records_after:
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
            "try_cast(SQLDATE AS DATE)"
        ) if False else F.to_date("SQLDATE", "yyyyMMdd")
    )
    .withColumn(
        "checked_date_added",
        F.to_timestamp("DATEADDED", "yyyyMMddHHmmss")
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


from pyspark.sql import functions as F
from delta.tables import DeltaTable

REJECTED_TABLE = "workspace.eventpulse_audit.rejected_events"

print("EVENTPULSE — REJECTED EVENTS STORAGE")
print("=" * 60)

records_before = spark.table(REJECTED_TABLE).count()

if new_rejections_count == 0:
    print("No new rejected records.")
    print("Delta MERGE skipped safely.")

else:
    target_df = spark.table(REJECTED_TABLE)

    prepared_rejections_df = (
        new_rejections_df
        .withColumn(
            "event_id",
            F.col("checked_event_id")
        )
        .withColumn(
            "event_date",
            F.col("checked_event_date")
        )
        .withColumn(
            "date_added",
            F.col("checked_date_added")
        )
        .withColumn(
            "rejection_reason",
            F.col("rejection_reason_check")
        )
        .withColumn(
            "record_status",
            F.lit("REJECTED")
        )
    )

    prepared_rejections_df = prepared_rejections_df.select(
        *[
            F.col(column)
            .cast(target_df.schema[column].dataType)
            .alias(column)
            for column in target_df.columns
        ]
    )

    prepared_rejections_df = (
        add_rejection_hash(prepared_rejections_df)
        .dropDuplicates(["rejection_hash"])
    )

    existing_rejections_df = (
        add_rejection_hash(target_df)
        .select("rejection_hash")
    )

    target = DeltaTable.forName(spark, REJECTED_TABLE)

    hash_fields = [
        "source_file",
        *gdelt_columns
    ]

    hash_struct = ", ".join(
        f"target.`{column}`" for column in hash_fields
    )

    target_hash_expression = (
        "sha2(to_json(named_struct("
        + ", ".join(
            f"'{column}', target.`{column}`"
            for column in hash_fields
        )
        + "), map('ignoreNullFields', 'false')), 256)"
    )

    (
        target.alias("target")
        .merge(
            prepared_rejections_df.alias("source"),
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

    print("Rejected events MERGE executed.")

records_after = spark.table(REJECTED_TABLE).count()

print(f"Rejected records before: {records_before:,}")
print(f"Rejected records after: {records_after:,}")

if new_rejections_count == 0 and records_after != records_before:
    raise ValueError("Rejected events changed during a no-op run")

print("Rejected events storage: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

sample_df = (
    spark.table(BRONZE_TABLE)
    .limit(1)
)

synthetic_invalid_df = (
    sample_df
    .withColumn("GLOBALEVENTID", F.lit("INVALID_TEST_ID"))
    .withColumn(
        "source_file",
        F.lit("synthetic://eventpulse/rejection_test.csv")
    )
    .withColumn(
        "batch_id",
        F.lit("SYNTHETIC_REJECTION_TEST")
    )
)

synthetic_validation_df = (
    synthetic_invalid_df
    .withColumn(
        "checked_event_id",
        F.expr(
            "try_cast(NULLIF(TRIM(GLOBALEVENTID), '') AS BIGINT)"
        )
    )
    .withColumn(
        "checked_event_date",
        F.to_date("SQLDATE", "yyyyMMdd")
    )
    .withColumn(
        "checked_date_added",
        F.to_timestamp("DATEADDED", "yyyyMMddHHmmss")
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

synthetic_result = synthetic_validation_df.select(
    "GLOBALEVENTID",
    "rejection_reason_check"
).first()

print("EVENTPULSE — SYNTHETIC REJECTION TEST")
print("=" * 60)
print(f"Event ID: {synthetic_result['GLOBALEVENTID']}")
print(f"Rejection reason: {synthetic_result['rejection_reason_check']}")

assert (
    "Invalid event ID"
    in synthetic_result["rejection_reason_check"]
)

print("Synthetic rejection validation: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

TEST_TABLE = "workspace.eventpulse_audit.rejection_merge_test"
REJECTED_TABLE = "workspace.eventpulse_audit.rejected_events"

rejection_schema_df = spark.table(REJECTED_TABLE).limit(0)

if spark.catalog.tableExists(TEST_TABLE):
    raise ValueError(
        f"Test table already exists: {TEST_TABLE}. "
        "Review it before proceeding."
    )

(
    rejection_schema_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(TEST_TABLE)
)

test_df = spark.table(TEST_TABLE)

print("EVENTPULSE — REJECTION MERGE TEST SETUP")
print("=" * 60)
print(f"Test table: {TEST_TABLE}")
print(f"Columns: {len(test_df.columns)}")
print(f"Initial records: {test_df.count():,}")

assert test_df.count() == 0
assert len(test_df.columns) == len(rejection_schema_df.columns)

print("Rejection test setup: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F
from delta.tables import DeltaTable

TEST_TABLE = "workspace.eventpulse_audit.rejection_merge_test"

target_df = spark.table(TEST_TABLE)

prepared_test_df = (
    synthetic_validation_df
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
            F.col(column)
            .cast(target_df.schema[column].dataType)
            .alias(column)
            for column in target_df.columns
        ]
    )
)

test_source_df = (
    add_rejection_hash(prepared_test_df)
    .dropDuplicates(["rejection_hash"])
)

hash_fields = ["source_file", *gdelt_columns]

target_hash_expression = (
    "sha2(to_json(named_struct("
    + ", ".join(
        f"'{column}', target.`{column}`"
        for column in hash_fields
    )
    + "), map('ignoreNullFields', 'false')), 256)"
)

records_before = target_df.count()

(
    DeltaTable.forName(spark, TEST_TABLE)
    .alias("target")
    .merge(
        test_source_df.alias("source"),
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

result_df = spark.table(TEST_TABLE)

records_after = result_df.count()

print("EVENTPULSE — REJECTION MERGE TEST")
print("=" * 60)
print(f"Records before: {records_before}")
print(f"Records after: {records_after}")

assert records_after == 1

print("Synthetic rejection MERGE: PASSED")


# COMMAND ----------


from delta.tables import DeltaTable
from pyspark.sql import functions as F

TEST_TABLE = "workspace.eventpulse_audit.rejection_merge_test"

records_before = spark.table(TEST_TABLE).count()

assert records_before == 1, (
    f"Expected 1 record before retry, found {records_before}"
)

(
    DeltaTable.forName(spark, TEST_TABLE)
    .alias("target")
    .merge(
        test_source_df.alias("source"),
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

result_df = spark.table(TEST_TABLE)

records_after = result_df.count()

stored_hashes_df = add_rejection_hash(result_df)

unique_hashes = (
    stored_hashes_df
    .select("rejection_hash")
    .distinct()
    .count()
)

validation_passed = (
    records_before == 1
    and records_after == 1
    and unique_hashes == 1
)

print("EVENTPULSE — REJECTION MERGE IDEMPOTENCY")
print("=" * 60)
print(f"Records before retry: {records_before}")
print(f"Records after retry: {records_after}")
print(f"Unique rejection hashes: {unique_hashes}")
print(f"Idempotency passed: {validation_passed}")

if not validation_passed:
    raise ValueError("Rejection MERGE idempotency test failed")


# COMMAND ----------


from pyspark.sql import functions as F

TEST_TABLE = "workspace.eventpulse_audit.rejection_merge_test"

test_df = spark.table(TEST_TABLE)

result = test_df.select(
    "GLOBALEVENTID",
    "source_file",
    "batch_id",
    "record_status",
    "rejection_reason",
    "event_id"
).first()

if result is None:
    raise ValueError("No synthetic rejection found")

checks = {
    "Invalid event ID preserved": (
        result["GLOBALEVENTID"] == "INVALID_TEST_ID"
    ),
    "Source file preserved": (
        result["source_file"]
        == "synthetic://eventpulse/rejection_test.csv"
    ),
    "Batch ID preserved": (
        result["batch_id"] == "SYNTHETIC_REJECTION_TEST"
    ),
    "Record classified as rejected": (
        result["record_status"] == "REJECTED"
    ),
    "Rejection reason preserved": (
        "Invalid event ID" in result["rejection_reason"]
    ),
    "Invalid numeric ID stored as NULL": (
        result["event_id"] is None
    )
}

print("EVENTPULSE — REJECTION TRACEABILITY TEST")
print("=" * 60)

for check, passed in checks.items():
    print(f"{check}: {'PASSED' if passed else 'FAILED'}")

validation_passed = all(checks.values())

print(f"\nTraceability validation: {validation_passed}")

if not validation_passed:
    raise ValueError("Rejection traceability validation failed")


# COMMAND ----------


from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.eventpulse_bronze.events_raw"
SILVER_TABLE = "workspace.eventpulse_silver.events_clean"
REJECTED_TABLE = "workspace.eventpulse_audit.rejected_events"
TEST_TABLE = "workspace.eventpulse_audit.rejection_merge_test"

bronze_df = spark.table(BRONZE_TABLE)
silver_df = spark.table(SILVER_TABLE)
rejected_df = spark.table(REJECTED_TABLE)
test_df = spark.table(TEST_TABLE)

bronze_count = bronze_df.count()
silver_count = silver_df.count()
silver_unique = silver_df.select("event_id").distinct().count()
rejected_count = rejected_df.count()
test_count = test_df.count()

bronze_ids = bronze_df.select(
    F.expr("try_cast(GLOBALEVENTID AS BIGINT)").alias("event_id")
)

missing_in_silver = (
    bronze_ids
    .filter(F.col("event_id").isNotNull())
    .join(
        silver_df.select("event_id"),
        "event_id",
        "left_anti"
    )
    .count()
)

checks = {
    "Bronze records preserved": bronze_count == 2155,
    "Silver records preserved": silver_count == 2155,
    "Silver event IDs unique": silver_count == silver_unique,
    "Production rejected records unchanged": rejected_count == 0,
    "Synthetic rejection isolated": test_count == 1,
    "All valid Bronze IDs represented": missing_in_silver == 0
}

print("EVENTPULSE — SILVER INTEGRITY AUDIT")
print("=" * 60)

for name, passed in checks.items():
    print(f"{name}: {'PASSED' if passed else 'FAILED'}")

validation_passed = all(checks.values())

print(f"\nSilver integrity audit: {validation_passed}")

if not validation_passed:
    raise ValueError("Silver integrity audit failed")


# COMMAND ----------


from pyspark.sql import functions as F

SILVER_TABLE = "workspace.eventpulse_silver.events_clean"
TEST_TABLE = "workspace.eventpulse_audit.silver_merge_test"

if spark.catalog.tableExists(TEST_TABLE):
    raise ValueError(
        f"Test table already exists: {TEST_TABLE}"
    )

test_source_df = (
    spark.table(SILVER_TABLE)
    .orderBy("event_id")
    .limit(5)
)

(
    test_source_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(TEST_TABLE)
)

test_df = spark.table(TEST_TABLE)

record_count = test_df.count()
unique_count = test_df.select("event_id").distinct().count()

print("EVENTPULSE — SILVER MERGE TEST SETUP")
print("=" * 60)
print(f"Test table: {TEST_TABLE}")
print(f"Records: {record_count}")
print(f"Unique events: {unique_count}")

assert record_count == 5
assert unique_count == 5

print("Silver MERGE test setup: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

TEST_TABLE = "workspace.eventpulse_audit.silver_merge_test"

test_df = spark.table(TEST_TABLE)

test_events = (
    test_df
    .orderBy("event_id")
    .limit(3)
    .collect()
)

newer_id = test_events[0]["event_id"]
older_id = test_events[1]["event_id"]

existing_ids = {
    row["event_id"]
    for row in spark.table(SILVER_TABLE)
        .select("event_id")
        .collect()
}

synthetic_new_id = max(existing_ids) + 1000000

newer_df = (
    test_df
    .filter(F.col("event_id") == newer_id)
    .withColumn(
        "date_added",
        F.expr("date_added + INTERVAL 1 DAY")
    )
    .withColumn("num_mentions", F.lit(999))
)

older_df = (
    test_df
    .filter(F.col("event_id") == older_id)
    .withColumn(
        "date_added",
        F.expr("date_added - INTERVAL 1 DAY")
    )
    .withColumn("num_mentions", F.lit(888))
)

new_df = (
    test_df
    .filter(F.col("event_id") == test_events[2]["event_id"])
    .withColumn("event_id", F.lit(synthetic_new_id))
    .withColumn("GLOBALEVENTID", F.lit(str(synthetic_new_id)))
)

synthetic_merge_df = newer_df.unionByName(
    older_df
).unionByName(new_df)

print("EVENTPULSE — SYNTHETIC SILVER VERSIONS")
print("=" * 60)
print(f"Newer version event ID: {newer_id}")
print(f"Older version event ID: {older_id}")
print(f"New event ID: {synthetic_new_id}")
print(f"Prepared records: {synthetic_merge_df.count()}")

assert synthetic_merge_df.count() == 3
assert synthetic_merge_df.select("event_id").distinct().count() == 3

print("Synthetic Silver versions: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

TEST_TABLE = "workspace.eventpulse_audit.silver_merge_test"

target_versions_df = (
    spark.table(TEST_TABLE)
    .select(
        "event_id",
        F.col("date_added").alias("target_date_added")
    )
)

synthetic_decisions_df = (
    synthetic_merge_df
    .join(target_versions_df, "event_id", "left")
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

decisions = {
    row["merge_action"]: row["count"]
    for row in (
        synthetic_decisions_df
        .groupBy("merge_action")
        .count()
        .collect()
    )
}

print("EVENTPULSE — SILVER MERGE DECISION TEST")
print("=" * 60)

for action, count in sorted(decisions.items()):
    print(f"{action}: {count}")

validation_passed = (
    decisions.get("INSERT", 0) == 1
    and decisions.get("UPDATE", 0) == 1
    and decisions.get("SKIP_OLDER", 0) == 1
    and sum(decisions.values()) == 3
)

print(f"Merge decision validation: {validation_passed}")

if not validation_passed:
    raise ValueError("Synthetic MERGE decisions are incorrect")


# COMMAND ----------


from delta.tables import DeltaTable
from pyspark.sql import functions as F

TEST_TABLE = "workspace.eventpulse_audit.silver_merge_test"

eligible_df = (
    synthetic_decisions_df
    .filter(F.col("merge_action").isin("INSERT", "UPDATE"))
    .select(*synthetic_merge_df.columns)
)

records_before = spark.table(TEST_TABLE).count()

(
    DeltaTable.forName(spark, TEST_TABLE)
    .alias("target")
    .merge(
        eligible_df.alias("source"),
        "target.event_id = source.event_id"
    )
    .whenMatchedUpdateAll(
        condition="source.date_added > target.date_added"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

result_df = spark.table(TEST_TABLE)

records_after = result_df.count()
unique_after = result_df.select("event_id").distinct().count()

updated_record = (
    result_df
    .filter(F.col("event_id") == newer_id)
    .select("num_mentions")
    .first()
)

older_record = (
    result_df
    .filter(F.col("event_id") == older_id)
    .select("num_mentions")
    .first()
)

original_older_record = (
    test_df
    .filter(F.col("event_id") == older_id)
    .select("num_mentions")
    .first()
)

new_record_count = result_df.filter(
    F.col("event_id") == synthetic_new_id
).count()

checks = {
    "One new event inserted": records_after == records_before + 1,
    "No duplicate event IDs": records_after == unique_after,
    "Newer version updated": updated_record["num_mentions"] == 999,
    "Older version preserved": (
        older_record["num_mentions"]
        == original_older_record["num_mentions"]
    ),
    "Synthetic new event exists": new_record_count == 1
}

print("EVENTPULSE — SYNTHETIC SILVER MERGE")
print("=" * 60)
print(f"Records before: {records_before}")
print(f"Records after: {records_after}")

for name, passed in checks.items():
    print(f"{name}: {'PASSED' if passed else 'FAILED'}")

validation_passed = all(checks.values())

print(f"Synthetic Silver MERGE: {validation_passed}")

if not validation_passed:
    raise ValueError("Synthetic Silver MERGE validation failed")


# COMMAND ----------


from delta.tables import DeltaTable
from pyspark.sql import functions as F

TEST_TABLE = "workspace.eventpulse_audit.silver_merge_test"

before_df = spark.table(TEST_TABLE)

records_before = before_df.count()
unique_before = before_df.select("event_id").distinct().count()

state_before = {
    row["event_id"]: (
        row["date_added"],
        row["num_mentions"]
    )
    for row in before_df.select(
        "event_id",
        "date_added",
        "num_mentions"
    ).collect()
}

(
    DeltaTable.forName(spark, TEST_TABLE)
    .alias("target")
    .merge(
        eligible_df.alias("source"),
        "target.event_id = source.event_id"
    )
    .whenMatchedUpdateAll(
        condition="source.date_added > target.date_added"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

after_df = spark.table(TEST_TABLE)

records_after = after_df.count()
unique_after = after_df.select("event_id").distinct().count()

state_after = {
    row["event_id"]: (
        row["date_added"],
        row["num_mentions"]
    )
    for row in after_df.select(
        "event_id",
        "date_added",
        "num_mentions"
    ).collect()
}

checks = {
    "Record count preserved": records_before == records_after == 6,
    "Event IDs remain unique": unique_before == unique_after == 6,
    "Event values unchanged": state_before == state_after
}

print("EVENTPULSE — SILVER MERGE IDEMPOTENCY")
print("=" * 60)

for name, passed in checks.items():
    print(f"{name}: {'PASSED' if passed else 'FAILED'}")

validation_passed = all(checks.values())

print(f"Silver MERGE idempotency: {validation_passed}")

if not validation_passed:
    raise ValueError("Silver MERGE idempotency test failed")


# COMMAND ----------


print("EVENTPULSE — MERGE IDEMPOTENCY DIAGNOSTIC")
print("=" * 60)

all_event_ids = sorted(set(state_before) | set(state_after))

differences = []

for event_id in all_event_ids:
    before = state_before.get(event_id)
    after = state_after.get(event_id)

    if before != after:
        differences.append({
            "event_id": event_id,
            "before_date_added": str(before[0]) if before else None,
            "after_date_added": str(after[0]) if after else None,
            "before_num_mentions": before[1] if before else None,
            "after_num_mentions": after[1] if after else None
        })

print(f"Events with changed values: {len(differences)}")

if differences:
    display(spark.createDataFrame(differences))
else:
    print("No differences detected.")

print("Diagnostic completed.")


# COMMAND ----------


from pyspark.sql import functions as F

TEST_TABLE = "workspace.eventpulse_audit.silver_merge_test"

target_version = (
    spark.table(TEST_TABLE)
    .filter(F.col("event_id") == newer_id)
    .select("event_id", "date_added", "num_mentions")
    .first()
)

source_version = (
    eligible_df
    .filter(F.col("event_id") == newer_id)
    .select("event_id", "date_added", "num_mentions")
    .first()
)

print("EVENTPULSE — VERSION COMPARISON")
print("=" * 60)
print(f"Event ID: {newer_id}")
print(f"Target date: {target_version['date_added']}")
print(f"Source date: {source_version['date_added']}")
print(f"Target mentions: {target_version['num_mentions']}")
print(f"Source mentions: {source_version['num_mentions']}")

if source_version["date_added"] > target_version["date_added"]:
    print("Decision: UPDATE")
elif source_version["date_added"] == target_version["date_added"]:
    print("Decision: SKIP_SAME_TIMESTAMP")
else:
    print("Decision: SKIP_OLDER")

print("Version comparison: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

TEST_TABLE = "workspace.eventpulse_audit.silver_merge_test"
SOURCE_TABLE = "workspace.eventpulse_audit.silver_merge_source_test"

if spark.catalog.tableExists(SOURCE_TABLE):
    raise ValueError(
        f"Source test table already exists: {SOURCE_TABLE}"
    )

source_snapshot_df = (
    eligible_df
    .select(*spark.table(TEST_TABLE).columns)
)

source_snapshot_count = source_snapshot_df.count()
source_unique_count = (
    source_snapshot_df
    .select("event_id")
    .distinct()
    .count()
)

assert source_snapshot_count == 2
assert source_unique_count == 2

(
    source_snapshot_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(SOURCE_TABLE)
)

fixed_source_df = spark.table(SOURCE_TABLE)

fixed_count = fixed_source_df.count()
fixed_unique = fixed_source_df.select("event_id").distinct().count()

print("EVENTPULSE — FIXED MERGE SOURCE")
print("=" * 60)
print(f"Source records: {fixed_count}")
print(f"Unique event IDs: {fixed_unique}")

assert fixed_count == 2
assert fixed_unique == 2

display(
    fixed_source_df.select(
        "event_id",
        "date_added",
        "num_mentions"
    ).orderBy("event_id")
)

print("Fixed MERGE source: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

TEST_TABLE = "workspace.eventpulse_audit.silver_merge_test"
SOURCE_TABLE = "workspace.eventpulse_audit.silver_merge_source_test"

print("EVENTPULSE — MERGE SOURCE DIAGNOSTIC")
print("=" * 60)

print(f"Target records: {spark.table(TEST_TABLE).count()}")
print(f"Eligible source records: {eligible_df.count()}")
print(f"Source snapshot exists: {spark.catalog.tableExists(SOURCE_TABLE)}")

print("\nELIGIBLE SOURCE")
eligible_df.select(
    "event_id",
    "date_added",
    "num_mentions"
).show(truncate=False)

print("\nCURRENT TARGET")
spark.table(TEST_TABLE).select(
    "event_id",
    "date_added",
    "num_mentions"
).orderBy("event_id").show(truncate=False)

print("MERGE source diagnostic: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

SILVER_TABLE = "workspace.eventpulse_silver.events_clean"
SOURCE_TABLE = "workspace.eventpulse_audit.silver_merge_source_test"

if spark.catalog.tableExists(SOURCE_TABLE):
    raise ValueError(f"Source table already exists: {SOURCE_TABLE}")

base_df = spark.table(SILVER_TABLE)

existing_event_df = (
    base_df
    .filter(F.col("event_id") == newer_id)
    .withColumn(
        "date_added",
        F.expr("date_added + INTERVAL 5 DAYS")
    )
    .withColumn("num_mentions", F.lit(999))
)

new_event_df = (
    base_df
    .filter(F.col("event_id") == test_events[2]["event_id"])
    .withColumn("event_id", F.lit(synthetic_new_id))
    .withColumn("GLOBALEVENTID", F.lit(str(synthetic_new_id)))
)

fixed_source_data_df = existing_event_df.unionByName(new_event_df)

source_count = fixed_source_data_df.count()
unique_count = fixed_source_data_df.select("event_id").distinct().count()

assert source_count == 2
assert unique_count == 2

(
    fixed_source_data_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(SOURCE_TABLE)
)

fixed_source_df = spark.table(SOURCE_TABLE)

print("EVENTPULSE — STABLE MERGE SOURCE")
print("=" * 60)
print(f"Source records: {fixed_source_df.count()}")
print(f"Unique event IDs: {fixed_source_df.select('event_id').distinct().count()}")

display(
    fixed_source_df.select(
        "event_id",
        "date_added",
        "num_mentions"
    ).orderBy("event_id")
)

print("Stable MERGE source: PASSED")


# COMMAND ----------


from delta.tables import DeltaTable
from pyspark.sql import functions as F

TEST_TABLE = "workspace.eventpulse_audit.silver_merge_test"
SOURCE_TABLE = "workspace.eventpulse_audit.silver_merge_source_test"

fixed_source_df = spark.table(SOURCE_TABLE)

def execute_test_merge():
    (
        DeltaTable.forName(spark, TEST_TABLE)
        .alias("target")
        .merge(
            fixed_source_df.alias("source"),
            "target.event_id = source.event_id"
        )
        .whenMatchedUpdateAll(
            condition="source.date_added > target.date_added"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

execute_test_merge()

first_state = {
    row["event_id"]: row.asDict(recursive=True)
    for row in spark.table(TEST_TABLE).collect()
}

execute_test_merge()

second_state = {
    row["event_id"]: row.asDict(recursive=True)
    for row in spark.table(TEST_TABLE).collect()
}

records_after = len(second_state)

validation_passed = (
    len(first_state) == 6
    and records_after == 6
    and first_state == second_state
)

print("EVENTPULSE — STABLE SILVER MERGE IDEMPOTENCY")
print("=" * 60)
print(f"Records after first MERGE: {len(first_state)}")
print(f"Records after second MERGE: {records_after}")
print(f"All columns unchanged: {first_state == second_state}")
print(f"Idempotency passed: {validation_passed}")

if not validation_passed:
    raise ValueError("Stable Silver MERGE idempotency failed")


# COMMAND ----------


from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.eventpulse_bronze.events_raw"
SILVER_TABLE = "workspace.eventpulse_silver.events_clean"
REJECTED_TABLE = "workspace.eventpulse_audit.rejected_events"

bronze_df = spark.table(BRONZE_TABLE)
silver_df = spark.table(SILVER_TABLE)
rejected_df = spark.table(REJECTED_TABLE)

bronze_count = bronze_df.count()
silver_count = silver_df.count()
unique_count = silver_df.select("event_id").distinct().count()
rejected_count = rejected_df.count()

bronze_ids_df = bronze_df.select(
    F.expr("try_cast(GLOBALEVENTID AS BIGINT)").alias("event_id")
)

missing_count = (
    bronze_ids_df
    .filter(F.col("event_id").isNotNull())
    .join(
        silver_df.select("event_id"),
        "event_id",
        "left_anti"
    )
    .count()
)

checks = {
    "Bronze records": bronze_count == 2155,
    "Silver records": silver_count == 2155,
    "Unique Silver IDs": unique_count == 2155,
    "Production rejections": rejected_count == 0,
    "Missing events in Silver": missing_count == 0
}

print("EVENTPULSE — PRODUCTION INTEGRITY")
print("=" * 60)

for name, passed in checks.items():
    print(f"{name}: {'PASSED' if passed else 'FAILED'}")

validation_passed = all(checks.values())

print(f"Production integrity: {validation_passed}")

if not validation_passed:
    raise ValueError("Production integrity validation failed")
