# Databricks notebook source
# MAGIC %run "../00 - common/01- ingestion data"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

races_df = (
    spark.table(bronze_table)
)

# COMMAND ----------

#races_df.display()

# COMMAND ----------

from pyspark.sql.functions import col

races_selected_df=(
    races_df.select(
        col('circuitId'),
        col("raceName"),
        col("season"),
        col("round"),
        col("date"),
        col("Currret_timeStamp"),
        col("source_file")
    )
)

# COMMAND ----------

races_renamed_df = (
    races_selected_df.withColumnsRenamed({
        "date": "race_date",
        "raceName" : "race_name",
        "circuitId" : "circuit_id"
    })
)

# COMMAND ----------

#races_renamed_df.display()

# COMMAND ----------

races_validated_df = races_renamed_df.filter("circuit_id is not null")

# COMMAND ----------

races_dulicate_removed_df= races_validated_df.dropDuplicates(["circuit_id"])

# COMMAND ----------

from pyspark.sql.functions import initcap

races_final_df = (
    races_dulicate_removed_df.withColumn("race_name",initcap("race_name"))
                               )

# COMMAND ----------

(
    races_final_df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(silver_table)
)

# COMMAND ----------

#spark.table(silver_table).display()