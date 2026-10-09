# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run "../00 - common/01- ingestion data"

# COMMAND ----------

bronze_table =f"{catalog_name}.{bronze_schema}.results" 
silver_table =f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

results_df = (
    spark.read.table(bronze_table)
)

# COMMAND ----------

#results_df.display()
results_df.describe().display()

# COMMAND ----------

results_drop_df = (
    results_df.drop("url")
)

# COMMAND ----------

results_renamed_df = (
    results_drop_df.withColumnRenamed("constructorId","constructor_id")
            .withColumnRenamed("driverId","driver_id")
            .withColumnRenamed("raceName","race_name")
            .withColumnRenamed("positionText","finish_position_text")
            .withColumnRenamed("date","race_date")
            .withColumnRenamed("grid","grid_position")
            .withColumnRenamed("laps","completed_laps")
            .withColumnRenamed("number","car_number")
            .withColumnRenamed("position","finish_position")
)

# COMMAND ----------

results_filter_df = (
    results_renamed_df.filter("season is not Null")
    .filter("round is not null")
    .filter("constructor_id is not null")
    .filter("driver_id is not null")
)

# COMMAND ----------

#results_filter_df.display()
#results_filter_df.describe().display()
#display(results_df.count() - results_filter_df.count())

# COMMAND ----------

results_dublicates_removed_df = (
    results_filter_df.dropDuplicates(["season","round","driver_id","constructor_id"])
)

# COMMAND ----------

#display(results_filter_df.count() - results_dublicates_removed_df.count())

# COMMAND ----------

#results_dublicates_removed_df.describe().display()

# COMMAND ----------

from pyspark.sql.functions import initcap

results_capital_df = (
    results_dublicates_removed_df.withColumn("race_name",initcap("race_name"))
)

# COMMAND ----------

#results_capital_df.display()

# COMMAND ----------

(
    results_capital_df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(silver_table)
)

# COMMAND ----------

#spark.table(silver_table).display()   