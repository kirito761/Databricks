# Databricks notebook source
# MAGIC %run "../00 - common/01- ingestion data"

# COMMAND ----------

# MAGIC %run "../00 - common/02-bronze helper"

# COMMAND ----------

source_file = f'{landing_folder_path}sprints'
table_name = f'{catalog_name}.{bronze_schema}.sprints'

# COMMAND ----------

source_file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

sprints_schema =StructType([
    StructField("date",DateType()),
    StructField("raceName",StringType()),
    StructField("round",DoubleType()),
    StructField("season",DoubleType()),
    StructField("url",StringType()),
    StructField("constructorId",StringType()),
    StructField("driverId",StringType()),
    StructField("grid",DoubleType()),
    StructField("laps",DoubleType()),
    StructField("number",DoubleType()),
    StructField("points",DoubleType()),
    StructField("position",DoubleType()),
    StructField("positionText",StringType()),
    StructField("status",StringType())
])

# COMMAND ----------

sprints_df = (
    spark.read
        .format('json')
        .schema(sprints_schema)
        .option('multiLine','true')
        .option('mode','FAILFAST')
        .load(source_file)
)

# COMMAND ----------

sprints_df = ingest_metadata(sprints_df)

# COMMAND ----------

sprints_df.display()

# COMMAND ----------

(
    sprints_df.write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(table_name)
)

# COMMAND ----------

spark.table(table_name).display()