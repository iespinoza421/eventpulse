# Databricks notebook source

from pyspark.sql import functions as F

df = spark.range(1, 11)

df = df.withColumn("number_squared", F.col("id") ** 2)

display(df)


# COMMAND ----------


catalog = "workspace"

required_schemas = {
    "eventpulse_bronze",
    "eventpulse_silver",
    "eventpulse_gold",
    "eventpulse_audit",
}

schemas = spark.sql(
    f"SHOW SCHEMAS IN {catalog}"
)

available_schemas = {
    row.databaseName
    for row in schemas.collect()
}

missing_schemas = required_schemas - available_schemas

if missing_schemas:
    raise ValueError(
        f"Missing schemas: {sorted(missing_schemas)}"
    )

print("EventPulse Unity Catalog validation passed.")

for schema in sorted(required_schemas):
    print(f"{catalog}.{schema}")
