# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run "../00 - common/01- ingestion data"

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_races"
silver_races_table = f"{catalog_name}.{silver_schema}.races"
silver_circuit_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

races_df = spark.table(silver_races_table)
circuit_df = spark.table(silver_circuit_table)

# COMMAND ----------

dim_races_df = (
    races_df.join(
        circuit_df,
        races_df.circuit_id == circuit_df.circuit_id,
        "inner"
    )
    .select(
        races_df.season,
        races_df.round,
        races_df.race_name,
        races_df.race_date,
        circuit_df.circuit_name,
        circuit_df.locality,
        circuit_df.country
    )
)

# COMMAND ----------

#dim_races_df.display()

# COMMAND ----------

(
    dim_races_df.write.mode("overwrite")
    .format("delta")
    .saveAsTable(target_table)
)

# COMMAND ----------

#spark.table(target_table).display()