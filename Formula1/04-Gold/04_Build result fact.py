# Databricks notebook source
# MAGIC %run "../00 - common/01- ingestion data"

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.fact_session_result"
results_silver_table= f"{catalog_name}.{silver_schema}.results"
sprints_silver_table = f"{catalog_name}.{silver_schema}.sprints"

# COMMAND ----------

from pyspark.sql.functions import lit

results_df= spark.table(results_silver_table).withColumn("session_type",lit("race")).drop("race_name","race_date","Currret_timeStamp","source_file")
sprints_df = spark.table(sprints_silver_table).withColumn("session_type",lit("sprints")).drop("race_name","race_date","Currret_timeStamp","source_file")

# COMMAND ----------

#results_df.display()

# COMMAND ----------

#sprints_df.display()

# COMMAND ----------

fact_results_sprints_df = results_df.unionByName(sprints_df)

# COMMAND ----------

#fact_results_sprints_df.display()

# COMMAND ----------

from pyspark.sql.functions import col
fact_final_df = (
    fact_results_sprints_df
    .withColumn("is_win",col("finish_position") == 1 )
    .withColumn("is_podium", col("finish_position").isin([1,2,3]))
    .withColumn("is_points", col("points")>0)
)

# COMMAND ----------

(
#fact_final_df
#.display()
)

# COMMAND ----------

(
    fact_final_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)   
)

# COMMAND ----------

#spark.table(target_table).display()