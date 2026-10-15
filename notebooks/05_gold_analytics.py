# Databricks notebook source

from pyspark.sql import functions as F

SILVER_TABLE = "workspace.eventpulse_silver.events_clean"

silver_df = spark.table(SILVER_TABLE)

daily_activity_df = (
    silver_df
    .groupBy("event_date")
    .agg(
        F.count("*").alias("total_events"),
        F.countDistinct("event_id").alias("unique_events"),
        F.sum("num_mentions").alias("total_mentions"),
        F.sum("num_sources").alias("total_sources"),
        F.sum("num_articles").alias("total_articles"),
        F.avg("avg_tone").alias("average_tone")
    )
    .orderBy("event_date")
)

print("GOLD — DAILY EVENT ACTIVITY")
print("=" * 60)
print(f"Source events: {silver_df.count():,}")
print(f"Dates represented: {daily_activity_df.count():,}")

display(daily_activity_df)


# COMMAND ----------


from pyspark.sql import functions as F

silver_count = silver_df.count()

silver_unique = (
    silver_df
    .select("event_id")
    .distinct()
    .count()
)
gold_summary = daily_activity_df.agg(
    F.sum("total_events").alias("aggregated_events"),
    F.sum("unique_events").alias("aggregated_unique_events"),
    F.count("*").alias("total_dates"),
    F.sum(
        F.when(F.col("total_events") <= 0, 1).otherwise(0)
    ).alias("invalid_groups")
).first()

if gold_summary is None:
    raise ValueError("Gold aggregation returned no results")

aggregated_events = gold_summary["aggregated_events"]
aggregated_unique = gold_summary["aggregated_unique_events"]
total_dates = gold_summary["total_dates"]
invalid_groups = gold_summary["invalid_groups"]

reconciliation_passed = (
    silver_count == silver_unique
    and silver_count == aggregated_events
    and silver_count == aggregated_unique
    and invalid_groups == 0
)

print("SILVER TO GOLD RECONCILIATION")
print("=" * 60)
print(f"Silver records: {silver_count:,}")
print(f"Gold aggregated events: {aggregated_events:,}")
print(f"Gold unique events: {aggregated_unique:,}")
print(f"Dates represented: {total_dates:,}")
print(f"Invalid groups: {invalid_groups:,}")
print(f"Reconciliation passed: {reconciliation_passed}")

if not reconciliation_passed:
    raise ValueError("Silver-to-Gold reconciliation failed")


# COMMAND ----------


from pyspark.sql import functions as F

GOLD_TABLE = "workspace.eventpulse_gold.daily_event_activity"
SILVER_TABLE = "workspace.eventpulse_silver.events_clean"

silver_df = spark.table(SILVER_TABLE)

silver_count = silver_df.count()
silver_unique = silver_df.select("event_id").distinct().count()

prepared_summary = daily_activity_df.agg(
    F.count("*").alias("date_groups"),
    F.sum("total_events").alias("total_events"),
    F.sum("unique_events").alias("unique_events")
).first()

reconciliation_passed = (
    silver_count == silver_unique
    and prepared_summary["total_events"] == silver_count
    and prepared_summary["unique_events"] == silver_count
)

print("EVENTPULSE — DAILY GOLD REFRESH")
print("=" * 60)
print(f"Silver events: {silver_count:,}")
print(f"Prepared events: {prepared_summary['total_events']:,}")
print(f"Reconciliation: {reconciliation_passed}")

if not reconciliation_passed:
    raise ValueError("Daily Gold reconciliation failed")

(
    daily_activity_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE)
)

saved_df = spark.table(GOLD_TABLE)

saved_summary = saved_df.agg(
    F.count("*").alias("date_groups"),
    F.sum("total_events").alias("total_events"),
    F.sum("unique_events").alias("unique_events")
).first()

validation_passed = (
    saved_summary["total_events"] == silver_count
    and saved_summary["unique_events"] == silver_count
)

print(f"Date groups: {saved_summary['date_groups']:,}")
print(f"Events represented: {saved_summary['total_events']:,}")
print(f"Unique events represented: {saved_summary['unique_events']:,}")
print(f"Daily Gold refresh: {validation_passed}")

if not validation_passed:
    raise ValueError("Daily Gold refresh validation failed")


# COMMAND ----------


from pyspark.sql import functions as F

SILVER_TABLE = "workspace.eventpulse_silver.events_clean"

silver_df = spark.table(SILVER_TABLE)

country_profile_df = (
    silver_df
    .withColumn(
        "country_code",
        F.when(
            F.col("action_country_code").isNull()
            | (F.trim(F.col("action_country_code")) == ""),
            "UNKNOWN"
        ).otherwise(F.trim(F.col("action_country_code")))
    )
    .groupBy("country_code")
    .agg(
        F.count("*").alias("total_events"),
        F.countDistinct("event_id").alias("unique_events")
    )
    .orderBy(F.desc("total_events"))
)

print("EVENTPULSE — COUNTRY CODE PROFILE")
print("=" * 60)
print(f"Distinct country codes: {country_profile_df.count():,}")

display(country_profile_df.limit(20))


# COMMAND ----------


top_countries = country_profile_df.limit(15)

print("TOP 15 GEOGRAPHIC CODES")
print("=" * 60)

top_countries.show(15, truncate=False)


# COMMAND ----------


from pyspark.sql import functions as F

LOOKUP_PATH = (
    "/Volumes/workspace/eventpulse_bronze/"
    "raw_files/fips_country_lookup.csv"
)

SILVER_TABLE = "workspace.eventpulse_silver.events_clean"

country_lookup_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(LOOKUP_PATH)
)

silver_df = spark.table(SILVER_TABLE)

country_codes_df = (
    silver_df
    .select(
        F.coalesce(
            F.nullif(F.trim(F.col("action_country_code")), F.lit("")),
            F.lit("UNKNOWN")
        ).alias("country_code")
    )
    .distinct()
)

country_validation_df = (
    country_codes_df.alias("events")
    .join(
        country_lookup_df.alias("lookup"),
        F.col("events.country_code") == F.col("lookup.country_code"),
        "left"
    )
    .select(
        F.col("events.country_code"),
        F.col("lookup.country_name")
    )
)

reference_count = country_lookup_df.count()
reference_unique = country_lookup_df.select("country_code").distinct().count()
total_codes = country_codes_df.count()

unmapped_df = country_validation_df.filter(
    F.col("country_name").isNull()
)

unmapped_count = unmapped_df.count()

print("EVENTPULSE — COUNTRY LOOKUP VALIDATION")
print("=" * 60)
print(f"Reference entries: {reference_count:,}")
print(f"Unique reference codes: {reference_unique:,}")
print(f"Geographic codes in Silver: {total_codes:,}")
print(f"Unmapped codes: {unmapped_count:,}")

if reference_count != 274:
    raise ValueError("Unexpected FIPS reference count")

if reference_unique != reference_count:
    raise ValueError("Duplicate country codes in reference")

print("\nUNMAPPED GEOGRAPHIC CODES")
unmapped_df.show(30, truncate=False)

print("Country lookup validation: PASSED")


# COMMAND ----------


from pyspark.sql import functions as F

DIM_COUNTRY = "workspace.eventpulse_gold.dim_country"

country_reference_df = country_lookup_df.select(
    F.trim("country_code").alias("country_code"),
    F.trim("country_name").alias("country_name")
)

unknown_df = spark.createDataFrame(
    [("UNKNOWN", "Unknown / Unspecified")],
    ["country_code", "country_name"]
)

dim_country_df = country_reference_df.unionByName(unknown_df)

total_countries = dim_country_df.count()
unique_codes = dim_country_df.select("country_code").distinct().count()

invalid_countries = dim_country_df.filter(
    F.col("country_code").isNull()
    | (F.col("country_code") == "")
    | F.col("country_name").isNull()
    | (F.col("country_name") == "")
).count()

validation_passed = (
    total_countries > 0
    and unique_codes == total_countries
    and invalid_countries == 0
)

print("EVENTPULSE — COUNTRY DIMENSION")
print("=" * 60)
print(f"Reference dimension records: {total_countries}")
print(f"Unique country codes: {unique_codes}")
print(f"Invalid country records: {invalid_countries}")
print(f"Country dimension validation: {validation_passed}")

if not validation_passed:
    raise ValueError("Country reference dimension validation failed")

display(dim_country_df.orderBy("country_code").limit(15))


# COMMAND ----------


if spark.catalog.tableExists(DIM_COUNTRY):
    print(f"Dimension already exists: {DIM_COUNTRY}")
    print("Initial load skipped.")
else:
    (
        dim_country_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(DIM_COUNTRY)
    )

    print(f"Country dimension created: {DIM_COUNTRY}")

saved_df = spark.table(DIM_COUNTRY)

country_count = saved_df.count()
unique_country_count = saved_df.select("country_code").distinct().count()
invalid_country_count = saved_df.filter(
    saved_df.country_code.isNull() |
    saved_df.country_name.isNull()
).count()

validation_passed = (
    country_count > 0
    and country_count == unique_country_count
    and invalid_country_count == 0
)

print("EVENTPULSE — COUNTRY DIMENSION VALIDATION")
print("=" * 60)
print(f"Country records: {country_count}")
print(f"Unique country codes: {unique_country_count}")
print(f"Invalid country records: {invalid_country_count}")
print(f"Country dimension validation: {validation_passed}")

if not validation_passed:
    raise ValueError("Country dimension validation failed")


# COMMAND ----------


from pyspark.sql import functions as F

DIM_COUNTRY = "workspace.eventpulse_gold.dim_country"
OVERRIDES_TABLE = "workspace.eventpulse_gold.country_code_overrides"

verified_countries_df = (
    spark.table(OVERRIDES_TABLE)
    .select("country_code", "country_name")
)

verified_countries_df.createOrReplaceTempView(
    "eventpulse_verified_countries"
)

spark.sql(f"""
MERGE INTO {DIM_COUNTRY} AS target
USING eventpulse_verified_countries AS source
ON target.country_code = source.country_code
WHEN NOT MATCHED THEN
    INSERT (country_code, country_name)
    VALUES (source.country_code, source.country_name)
""")

verified_count = verified_countries_df.count()

matched_count = (
    verified_countries_df.alias("source")
    .join(
        spark.table(DIM_COUNTRY).alias("target"),
        "country_code",
        "inner"
    )
    .filter(
        F.col("source.country_name") == F.col("target.country_name")
    )
    .count()
)

validation_passed = verified_count == matched_count

print("EVENTPULSE — COUNTRY DIMENSION MERGE")
print("=" * 60)
print(f"Verified geographic codes: {verified_count}")
print(f"Codes present in dimension: {matched_count}")
print(f"Country dimension merge: {validation_passed}")

if not validation_passed:
    raise ValueError("Country dimension merge validation failed")


# COMMAND ----------


from pyspark.sql import functions as F

SILVER_TABLE = "workspace.eventpulse_silver.events_clean"
DIM_COUNTRY = "workspace.eventpulse_gold.dim_country"

silver_df = spark.table(SILVER_TABLE)
dim_country_df = spark.table(DIM_COUNTRY)

events_with_country_df = (
    silver_df
    .withColumn(
        "country_code",
        F.coalesce(
            F.nullif(F.trim(F.col("action_country_code")), F.lit("")),
            F.lit("UNKNOWN")
        )
    )
    .join(dim_country_df, "country_code", "left")
    .withColumn(
        "country_name",
        F.when(
            F.col("country_code") == "UNKNOWN",
            F.lit("Unknown / Unspecified")
        )
        .when(
            F.col("country_name").isNull(),
            F.lit("UNMAPPED")
        )
        .otherwise(F.col("country_name"))
    )
)

unmapped_df = events_with_country_df.filter(
    F.col("country_name") == "UNMAPPED"
)

unmapped_count = unmapped_df.count()

country_activity_df = (
    events_with_country_df
    .groupBy("country_code", "country_name")
    .agg(
        F.count("*").alias("total_events"),
        F.countDistinct("event_id").alias("unique_events"),
        F.sum("num_mentions").alias("total_mentions"),
        F.sum("num_articles").alias("total_articles"),
        F.avg("avg_tone").alias("average_tone")
    )
    .orderBy(F.desc("total_events"))
)

print("EVENTPULSE — COUNTRY EVENT ACTIVITY")
print("=" * 60)
print(f"Source events: {silver_df.count():,}")
print(f"Geographic groups: {country_activity_df.count():,}")
print(f"Unmapped events: {unmapped_count:,}")
print(f"Country transformation: PASSED")

display(country_activity_df.limit(15))


# COMMAND ----------


from pyspark.sql import functions as F

AUDIT_TABLE = "workspace.eventpulse_audit.unmapped_country_codes"

unmapped_audit_df = (
    events_with_country_df
    .filter(F.col("country_name") == "UNMAPPED")
    .groupBy("country_code")
    .agg(
        F.count("*").alias("affected_events"),
        F.min("event_date").alias("first_event_date"),
        F.max("event_date").alias("last_event_date")
    )
)

if not spark.catalog.tableExists(AUDIT_TABLE):
    (
        unmapped_audit_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(AUDIT_TABLE)
    )
else:
    (
        unmapped_audit_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(AUDIT_TABLE)
    )

audit_count = spark.table(AUDIT_TABLE).count()

print("EVENTPULSE — GEOGRAPHIC QUALITY AUDIT")
print("=" * 60)
print(f"Unmapped geographic codes: {audit_count}")
print(f"Unmapped events: {unmapped_count}")
print("Geographic quality audit: PASSED")

display(spark.table(AUDIT_TABLE))


# COMMAND ----------


from pyspark.sql import functions as F

OVERRIDES_TABLE = "workspace.eventpulse_gold.country_code_overrides"

initial_overrides_df = spark.createDataFrame(
    [("RB", "Serbia")],
    ["country_code", "country_name"]
)

if not spark.catalog.tableExists(OVERRIDES_TABLE):
    (
        initial_overrides_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(OVERRIDES_TABLE)
    )

    print(f"Reference table created: {OVERRIDES_TABLE}")
else:
    print(f"Reference table already exists: {OVERRIDES_TABLE}")

overrides_df = spark.table(OVERRIDES_TABLE)

total_codes = overrides_df.count()
unique_codes = overrides_df.select("country_code").distinct().count()

invalid_codes = overrides_df.filter(
    F.col("country_code").isNull()
    | (F.trim("country_code") == "")
    | F.col("country_name").isNull()
    | (F.trim("country_name") == "")
).count()

validation_passed = (
    total_codes > 0
    and total_codes == unique_codes
    and invalid_codes == 0
)

print("EVENTPULSE — COUNTRY OVERRIDES REFERENCE")
print("=" * 60)
print(f"Verified country codes: {total_codes}")
print(f"Unique country codes: {unique_codes}")
print(f"Invalid records: {invalid_codes}")
print(f"Reference validation: {validation_passed}")

if not validation_passed:
    raise ValueError("Country overrides reference validation failed")


# COMMAND ----------


from pyspark.sql import functions as F

DIM_COUNTRY = "workspace.eventpulse_gold.dim_country"
OVERRIDES_TABLE = "workspace.eventpulse_gold.country_code_overrides"

additional_countries_df = (
    spark.table(OVERRIDES_TABLE)
    .select(
        F.trim("country_code").alias("country_code"),
        F.trim("country_name").alias("country_name")
    )
)

existing_countries_df = spark.table(DIM_COUNTRY)

new_countries_df = additional_countries_df.join(
    existing_countries_df.select("country_code"),
    "country_code",
    "left_anti"
)

print("EVENTPULSE — ADDITIONAL COUNTRY REFERENCE")
print("=" * 60)
print(f"Verified additional codes: {additional_countries_df.count()}")
print(f"Codes not yet in dimension: {new_countries_df.count()}")

display(new_countries_df)


# COMMAND ----------


DIM_COUNTRY = "workspace.eventpulse_gold.dim_country"

additional_countries_df.createOrReplaceTempView(
    "eventpulse_verified_countries"
)

spark.sql(f"""
MERGE INTO {DIM_COUNTRY} AS target
USING eventpulse_verified_countries AS source
ON target.country_code = source.country_code
WHEN NOT MATCHED THEN
    INSERT (country_code, country_name)
    VALUES (source.country_code, source.country_name)
""")

verified_codes = [
    row["country_code"]
    for row in additional_countries_df.select("country_code").collect()
]

saved_countries_df = spark.table(DIM_COUNTRY)

verified_count = (
    saved_countries_df
    .filter(F.col("country_code").isin(verified_codes))
    .count()
)

validation_passed = verified_count == len(verified_codes)

print("EVENTPULSE — COUNTRY DIMENSION MERGE")
print("=" * 60)
print(f"Verified additional codes: {len(verified_codes)}")
print(f"Codes present in dimension: {verified_count}")
print(f"Country dimension merge: {validation_passed}")

if not validation_passed:
    raise ValueError("Country dimension merge validation failed")


# COMMAND ----------


from pyspark.sql import functions as F

silver_count = silver_df.count()

silver_unique = (
    silver_df
    .select("event_id")
    .distinct()
    .count()
)

joined_count = events_with_country_df.count()

summary = country_activity_df.agg(
    F.sum("total_events").alias("aggregated_events"),
    F.sum("unique_events").alias("aggregated_unique_events"),
    F.count("*").alias("geographic_groups")
).first()

if summary is None:
    raise ValueError("Country aggregation returned no results")

aggregated_events = summary["aggregated_events"]
aggregated_unique = summary["aggregated_unique_events"]
geographic_groups = summary["geographic_groups"]

duplicate_country_codes = (
    dim_country_df
    .groupBy("country_code")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

reconciliation_passed = (
    silver_count == silver_unique
    and silver_count == joined_count
    and silver_count == aggregated_events
    and silver_count == aggregated_unique
    and duplicate_country_codes == 0
    and unmapped_count == 0
)

print("COUNTRY GOLD RECONCILIATION")
print("=" * 60)
print(f"Silver records: {silver_count:,}")
print(f"Records after JOIN: {joined_count:,}")
print(f"Aggregated events: {aggregated_events:,}")
print(f"Aggregated unique events: {aggregated_unique:,}")
print(f"Geographic groups: {geographic_groups:,}")
print(f"Duplicate dimension codes: {duplicate_country_codes:,}")
print(f"Unmapped records: {unmapped_count:,}")
print(f"Reconciliation passed: {reconciliation_passed}")

if not reconciliation_passed:
    raise ValueError("Country Gold reconciliation failed")


# COMMAND ----------


from pyspark.sql import functions as F

silver_count = spark.table(
    "workspace.eventpulse_silver.events_clean"
).count()

country_summary = country_activity_df.agg(
    F.count("*").alias("country_groups"),
    F.sum("total_events").alias("total_events"),
    F.sum("unique_events").alias("unique_events")
).first()

print("EVENTPULSE — COUNTRY GOLD PREPARATION")
print("=" * 60)
print(f"Silver events: {silver_count:,}")
print(f"Country groups: {country_summary['country_groups']:,}")
print(f"Prepared events: {country_summary['total_events']:,}")
print(f"Prepared unique events: {country_summary['unique_events']:,}")

validation_passed = (
    country_summary["total_events"] == silver_count
    and country_summary["unique_events"] == silver_count
)

print(f"Country preparation: {validation_passed}")


# COMMAND ----------


from pyspark.sql import functions as F

GOLD_TABLE = "workspace.eventpulse_gold.country_event_activity"
SILVER_TABLE = "workspace.eventpulse_silver.events_clean"


silver_df = spark.table(SILVER_TABLE)

silver_count = silver_df.count()
silver_unique_count = silver_df.select("event_id").distinct().count()

prepared_summary = country_activity_df.agg(
    F.sum("total_events").alias("total_events"),
    F.sum("unique_events").alias("unique_events")
).first()

reconciliation_passed = (
    silver_count == silver_unique_count
    and prepared_summary["total_events"] == silver_count
    and prepared_summary["unique_events"] == silver_count
)

print(f"Country reconciliation: {reconciliation_passed}")

if not reconciliation_passed:
    raise ValueError("Country reconciliation failed")


silver_count = spark.table(SILVER_TABLE).count()

prepared_summary = country_activity_df.agg(
    F.count("*").alias("geographic_groups"),
    F.sum("total_events").alias("total_events"),
    F.sum("unique_events").alias("unique_events")
).first()

print("EVENTPULSE — COUNTRY GOLD REFRESH")
print("=" * 60)
print(f"Silver events: {silver_count:,}")
print(f"Prepared Gold events: {prepared_summary['total_events']:,}")

if prepared_summary["total_events"] != silver_count:
    raise ValueError("Country Gold does not reconcile with Silver")

if prepared_summary["unique_events"] != silver_count:
    raise ValueError("Country Gold unique events do not reconcile with Silver")

(
    country_activity_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE)
)

saved_df = spark.table(GOLD_TABLE)

summary = saved_df.agg(
    F.count("*").alias("geographic_groups"),
    F.sum("total_events").alias("total_events"),
    F.sum("unique_events").alias("unique_events")
).first()

validation_passed = (
    summary["total_events"] == silver_count
    and summary["unique_events"] == silver_count
)

print(f"Geographic groups: {summary['geographic_groups']:,}")
print(f"Events represented: {summary['total_events']:,}")
print(f"Unique events represented: {summary['unique_events']:,}")
print(f"Country Gold refresh: {validation_passed}")

if not validation_passed:
    raise ValueError("Country Gold refresh validation failed")


# COMMAND ----------


from pyspark.sql import functions as F

SILVER_TABLE = "workspace.eventpulse_silver.events_clean"

silver_df = spark.table(SILVER_TABLE)

category_profile_df = (
    silver_df
    .groupBy("EventRootCode")
    .agg(
        F.count("*").alias("total_events"),
        F.countDistinct("event_id").alias("unique_events"),
        F.sum("num_mentions").alias("total_mentions"),
        F.avg("avg_tone").alias("average_tone")
    )
    .orderBy(F.desc("total_events"))
)

print("EVENTPULSE — CAMEO CATEGORY PROFILE")
print("=" * 60)
print(f"Source records: {silver_df.count():,}")
print(f"Distinct root categories: {category_profile_df.count():,}")

display(category_profile_df)


# COMMAND ----------


from pyspark.sql import functions as F

print("EVENTPULSE — CAMEO ROOT CODE VALIDATION")
print("=" * 60)

observed_codes = {
    row["EventRootCode"]
    for row in silver_df
        .select("EventRootCode")
        .distinct()
        .collect()
}

expected_codes = {
    f"{code:02d}"
    for code in range(1, 21)
}

missing_codes = sorted(expected_codes - observed_codes)
unexpected_codes = sorted(observed_codes - expected_codes)

print(f"Observed categories: {len(observed_codes)}")
print(f"Missing CAMEO root codes: {missing_codes}")
print(f"Unexpected codes: {unexpected_codes}")

category_profile_df.select(
    "EventRootCode",
    "total_events"
).show(25, truncate=False)

assert len(unexpected_codes) == 0, (
    f"Unexpected CAMEO codes: {unexpected_codes}"
)

print("CAMEO root code validation: PASSED")



# COMMAND ----------


from pyspark.sql import functions as F

CAMEO_PATH = (
    "/Volumes/workspace/eventpulse_bronze/"
    "raw_files/cameo_root_lookup.csv"
)

SILVER_TABLE = "workspace.eventpulse_silver.events_clean"

cameo_lookup_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(CAMEO_PATH)
    .select(
        F.trim("event_root_code").alias("event_root_code"),
        F.trim("category_name").alias("category_name")
    )
)

silver_df = spark.table(SILVER_TABLE)

reference_count = cameo_lookup_df.count()
unique_codes = cameo_lookup_df.select(
    "event_root_code"
).distinct().count()

missing_names = cameo_lookup_df.filter(
    F.col("category_name").isNull()
    | (F.col("category_name") == "")
).count()

expected_codes = {f"{i:02d}" for i in range(1, 21)}

actual_codes = {
    row["event_root_code"]
    for row in cameo_lookup_df.select("event_root_code").collect()
}

silver_codes_df = (
    silver_df
    .select(F.col("EventRootCode").alias("event_root_code"))
    .distinct()
)

unmapped_df = silver_codes_df.join(
    cameo_lookup_df.select("event_root_code"),
    "event_root_code",
    "left_anti"
)

unmapped_count = unmapped_df.count()

validation_passed = (
    reference_count == 20
    and unique_codes == 20
    and missing_names == 0
    and actual_codes == expected_codes
    and unmapped_count == 0
)

print("EVENTPULSE — CAMEO LOOKUP VALIDATION")
print("=" * 60)
print(f"Reference categories: {reference_count}")
print(f"Unique codes: {unique_codes}")
print(f"Missing descriptions: {missing_names}")
print(f"Codes observed in Silver: {silver_codes_df.count()}")
print(f"Unmapped Silver codes: {unmapped_count}")
print(f"Validation passed: {validation_passed}")

if not validation_passed:
    unmapped_df.show(truncate=False)
    raise ValueError("CAMEO lookup validation failed")

display(cameo_lookup_df.orderBy("event_root_code"))


# COMMAND ----------


from pyspark.sql import functions as F

DIM_CATEGORY = "workspace.eventpulse_gold.dim_event_category"

dim_category_df = (
    cameo_lookup_df
    .select(
        "event_root_code",
        "category_name"
    )
    .orderBy("event_root_code")
)

total_categories = dim_category_df.count()
unique_categories = (
    dim_category_df
    .select("event_root_code")
    .distinct()
    .count()
)

assert total_categories == 20
assert unique_categories == 20

if spark.catalog.tableExists(DIM_CATEGORY):
    print(f"Dimension already exists: {DIM_CATEGORY}")
    print("Initial load skipped.")
else:
    
    (
    dim_category_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(DIM_CATEGORY)
    )


    print(f"Dimension created: {DIM_CATEGORY}")

saved_df = spark.table(DIM_CATEGORY)

saved_count = saved_df.count()
saved_unique = saved_df.select(
    "event_root_code"
).distinct().count()

saved_codes = {
    row["event_root_code"]
    for row in saved_df.select("event_root_code").collect()
}

expected_codes = {f"{i:02d}" for i in range(1, 21)}

validation_passed = (
    saved_count == 20
    and saved_unique == 20
    and saved_codes == expected_codes
)

print("\nEVENTPULSE — CAMEO DIMENSION VALIDATION")
print("=" * 60)
print(f"Dimension records: {saved_count}")
print(f"Unique root codes: {saved_unique}")
print(f"Validation passed: {validation_passed}")

if not validation_passed:
    raise ValueError("CAMEO dimension validation failed")

display(saved_df.orderBy("event_root_code"))


# COMMAND ----------


from pyspark.sql import functions as F

SILVER_TABLE = "workspace.eventpulse_silver.events_clean"
DIM_CATEGORY = "workspace.eventpulse_gold.dim_event_category"

silver_df = spark.table(SILVER_TABLE)
dim_category_df = spark.table(DIM_CATEGORY)

events_with_category_df = (
    silver_df
    .withColumnRenamed("EventRootCode", "event_root_code")
    .join(
        dim_category_df,
        "event_root_code",
        "left"
    )
)

unmapped_count = events_with_category_df.filter(
    F.col("category_name").isNull()
).count()

if unmapped_count > 0:
    raise ValueError(
        f"Found {unmapped_count} events with unmapped CAMEO categories"
    )

category_activity_df = (
    events_with_category_df
    .groupBy("event_root_code", "category_name")
    .agg(
        F.count("*").alias("total_events"),
        F.countDistinct("event_id").alias("unique_events"),
        F.sum("num_mentions").alias("total_mentions"),
        F.sum("num_articles").alias("total_articles"),
        F.avg("avg_tone").alias("average_tone")
    )
    .orderBy(F.desc("total_events"))
)

print("GOLD — EVENT CATEGORY ACTIVITY")
print("=" * 60)
print(f"Source events: {silver_df.count():,}")
print(f"Category groups: {category_activity_df.count():,}")
print(f"Unmapped records: {unmapped_count:,}")

display(category_activity_df)


# COMMAND ----------


from pyspark.sql import functions as F

silver_count = silver_df.count()
silver_unique = silver_df.select("event_id").distinct().count()
joined_count = events_with_category_df.count()

summary = category_activity_df.agg(
    F.sum("total_events").alias("aggregated_events"),
    F.sum("unique_events").alias("aggregated_unique_events"),
    F.count("*").alias("category_groups")
).first()

aggregated_events = summary["aggregated_events"]
aggregated_unique = summary["aggregated_unique_events"]
category_groups = summary["category_groups"]

duplicate_category_codes = (
    dim_category_df
    .groupBy("event_root_code")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

dimension_categories = dim_category_df.select(
    "event_root_code"
).distinct().count()

reconciliation_passed = (
    silver_count == silver_unique
    and silver_count == joined_count
    and silver_count == aggregated_events
    and silver_count == aggregated_unique
    and 0 < category_groups <= dimension_categories
    and duplicate_category_codes == 0
    and unmapped_count == 0
)

print("EVENTPULSE — CATEGORY GOLD RECONCILIATION")
print("=" * 60)
print(f"Silver records: {silver_count:,}")
print(f"Records after JOIN: {joined_count:,}")
print(f"Aggregated events: {aggregated_events:,}")
print(f"Aggregated unique events: {aggregated_unique:,}")
print(f"Category groups: {category_groups}")
print(f"Available categories: {dimension_categories}")
print(f"Duplicate dimension codes: {duplicate_category_codes}")
print(f"Unmapped records: {unmapped_count}")
print(f"Reconciliation passed: {reconciliation_passed}")

if not reconciliation_passed:
    raise ValueError("Category Gold reconciliation failed")


# COMMAND ----------


from pyspark.sql import functions as F

GOLD_TABLE = "workspace.eventpulse_gold.event_category_activity"
SILVER_TABLE = "workspace.eventpulse_silver.events_clean"

silver_df = spark.table(SILVER_TABLE)

silver_count = silver_df.count()
silver_unique = silver_df.select("event_id").distinct().count()

prepared_summary = category_activity_df.agg(
    F.count("*").alias("category_groups"),
    F.sum("total_events").alias("total_events"),
    F.sum("unique_events").alias("unique_events")
).first()

reconciliation_passed = (
    silver_count == silver_unique
    and prepared_summary["total_events"] == silver_count
    and prepared_summary["unique_events"] == silver_count
)

print("EVENTPULSE — CATEGORY GOLD REFRESH")
print("=" * 60)
print(f"Silver events: {silver_count:,}")
print(f"Prepared events: {prepared_summary['total_events']:,}")
print(f"Category groups: {prepared_summary['category_groups']:,}")
print(f"Reconciliation: {reconciliation_passed}")

if not reconciliation_passed:
    raise ValueError("Category Gold reconciliation failed")

(
    category_activity_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE)
)

saved_df = spark.table(GOLD_TABLE)

saved_summary = saved_df.agg(
    F.count("*").alias("category_groups"),
    F.sum("total_events").alias("total_events"),
    F.sum("unique_events").alias("unique_events")
).first()

validation_passed = (
    saved_summary["total_events"] == silver_count
    and saved_summary["unique_events"] == silver_count
)

print(f"Saved category groups: {saved_summary['category_groups']:,}")
print(f"Events represented: {saved_summary['total_events']:,}")
print(f"Unique events represented: {saved_summary['unique_events']:,}")
print(f"Category Gold refresh: {validation_passed}")

if not validation_passed:
    raise ValueError("Category Gold refresh validation failed")


# COMMAND ----------


from pyspark.sql import functions as F

SILVER = "workspace.eventpulse_silver.events_clean"

GOLD_TABLES = {
    "Daily Activity": "workspace.eventpulse_gold.daily_event_activity",
    "Country Activity": "workspace.eventpulse_gold.country_event_activity",
    "Category Activity": "workspace.eventpulse_gold.event_category_activity"
}

silver_df = spark.table(SILVER)

silver_count = silver_df.count()
silver_unique = silver_df.select("event_id").distinct().count()

results = []

for name, table_name in GOLD_TABLES.items():

    df = spark.table(table_name)

    metrics = df.agg(
        F.count("*").alias("groups"),
        F.sum("total_events").alias("events"),
        F.sum("unique_events").alias("unique_events")
    ).first()

    passed = (
        metrics["events"] == silver_count
        and metrics["unique_events"] == silver_unique
    )

    results.append((
        name,
        metrics["groups"],
        metrics["events"],
        metrics["unique_events"],
        passed
    ))

print("EVENTPULSE — GOLD LAYER AUDIT")
print("=" * 65)
print(f"Silver records: {silver_count:,}")
print(f"Silver unique events: {silver_unique:,}")

audit_df = spark.createDataFrame(
    results,
    [
        "gold_table",
        "groups",
        "total_events",
        "unique_events",
        "reconciliation_passed"
    ]
)

display(audit_df)

if silver_count != silver_unique:
    raise ValueError("Silver contains duplicate event IDs")

if not all(row[4] for row in results):
    raise ValueError("Gold layer reconciliation failed")

print("Gold layer audit: PASSED")
