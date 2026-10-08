# Databricks notebook source
# MAGIC %run "../00 - common/01- ingestion data"

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_driver"
silver_driver_table= f"{catalog_name}.{silver_schema}.drivers"
ref_nationality_table = f"{catalog_name}.{gold_schema}.ref_nationality_region"

# COMMAND ----------

drivers_df = spark.read.table(silver_driver_table)
ref_nationality_df = spark.read.table(ref_nationality_table)

# COMMAND ----------

dim_drivers_df = drivers_df.join(
    ref_nationality_df,
    drivers_df.nationality == ref_nationality_df.nationality,
    "left"
).select(
    drivers_df.driver_id,
    drivers_df.driver_name,
    drivers_df.date_of_birth,
    drivers_df.nationality,
    ref_nationality_df.region
)

# COMMAND ----------

dim_drivers_df.display()

# COMMAND ----------

(
    dim_drivers_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

# COMMAND ----------

spark.table(target_table).display()