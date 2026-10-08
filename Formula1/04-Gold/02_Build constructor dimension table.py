# Databricks notebook source
# MAGIC %run "../00 - common/01- ingestion data"

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_constructor"
constructor_silver_table = f"{catalog_name}.{silver_schema}.constructors"
ref_nationality_region_table = f"{catalog_name}.{gold_schema}.ref_nationality_region"

# COMMAND ----------

constuctors_df =spark.table(constructor_silver_table)
ref_nationality_region_df = spark.table(ref_nationality_region_table)

# COMMAND ----------

dim_constructor_df = constuctors_df.join(
    ref_nationality_region_df,
    constuctors_df.nationality == ref_nationality_region_df.nationality,
    "left"
).select(
    constuctors_df.constructor_id,
    constuctors_df.constructor_name,
    constuctors_df.nationality,
    ref_nationality_region_df.region
)

# COMMAND ----------

dim_constructor_df.display()

# COMMAND ----------

from pyspark.sql.functions import col

dim_constructor_df_final= dim_constructor_df.select(
   
)

# COMMAND ----------

dim_constructor_df_final.display()

# COMMAND ----------

(
    dim_constructor_df.write.mode("overwrite").format("delta").saveAsTable(target_table)
)

# COMMAND ----------

spark.table(target_table).display()