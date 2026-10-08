# Databricks notebook source
# MAGIC %run "../00 - common/01- ingestion data"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

circuit_df = (
    spark.table(bronze_table)
)

# COMMAND ----------

circuit_df.display()

# COMMAND ----------

from pyspark.sql.functions import col

circuit_selected_df=(
    circuit_df.select(
        col('circuitId'),
        col("circuitName"),
        col("lat"),
        col("long"),
        col("locality"),
        col("country"),
        col("Currret_timeStamp"),
        col("source_file")
    )
)

# COMMAND ----------

circuit_renamed_df = (
    circuit_selected_df.withColumnsRenamed({
        "circuitId": "circuit_id",
        "circuitName": "circuit_name",
        "lat": "latitude",
        "long": "longitude",
    })
)

# COMMAND ----------

circuit_renamed_df.display()

# COMMAND ----------

circuit_validated_df = circuit_renamed_df.filter("circuit_id is not null")

# COMMAND ----------

circuit_dulicate_removed_df= circuit_validated_df.dropDuplicates(["circuit_id"])


# COMMAND ----------

from pyspark.sql.functions import initcap

circuit_final_df = (
    circuit_dulicate_removed_df.withColumn("circuit_name",initcap("circuit_name"))
                               .withColumn("locality",initcap("locality"))
)

# COMMAND ----------

(
    circuit_final_df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(silver_table)
)

# COMMAND ----------

spark.table(silver_table).display()