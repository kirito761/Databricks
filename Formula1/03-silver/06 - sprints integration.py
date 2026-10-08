# Databricks notebook source
# MAGIC %run "../00 - common/01- ingestion data"

# COMMAND ----------

bronze_table =f"{catalog_name}.{bronze_schema}.sprints" 
silver_table =f"{catalog_name}.{silver_schema}.sprints"

# COMMAND ----------

results_df = (
    spark.read.table(bronze_table)
)

# COMMAND ----------

results_df.display()
#results_df.describe().display()

# COMMAND ----------

from pyspark.sql.functions import initcap

reults_final_df = (
    results_df.drop("url")
              .withColumnRenamed("constructorId","constructor_id")
              .withColumnRenamed("driverId","driver_id")
              .withColumnRenamed("raceName","race_name")
              .withColumnRenamed("positionText","finish_position_text")
              .withColumnRenamed("date","race_date")
              .withColumnRenamed("grid","grid_position")
              .withColumnRenamed("laps","completed_laps")
              .withColumnRenamed("number","car_number")
              .withColumnRenamed("position","finish_position")
              .filter("season is not Null")
    .filter("round is not null")
    .filter("constructor_id is not null")
    .filter("driver_id is not null")
    .dropDuplicates(["season","round","driver_id","constructor_id"])
    .withColumn("race_name",initcap("race_name"))

)

# COMMAND ----------

reults_final_df.display()

# COMMAND ----------

display(reults_final_df.count() -results_df.count())

# COMMAND ----------

(
    reults_final_df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(silver_table)
)

# COMMAND ----------

spark.table(silver_table).display()   