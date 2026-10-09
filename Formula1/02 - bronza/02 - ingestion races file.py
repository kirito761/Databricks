# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run "../00 - common/01- ingestion data"

# COMMAND ----------

# MAGIC %run "../00 - common/02-bronze helper"

# COMMAND ----------

source_file = f"{landing_folder_path}/races.csv"
table_name = f"{catalog_name}.{bronze_schema}.races" 

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DoubleType, StructField, StructType,DateType
races_schema = StructType([
    StructField('season',IntegerType()),
    StructField('round',IntegerType()),
    StructField('url',StringType()),
    StructField('raceName',StringType()),
    StructField('date',DateType()),
    StructField('circuitId',StringType())
])

# COMMAND ----------

races_df = (
    spark.read
        .format('csv')
        .option('header', True)
        .schema(races_schema)
        .load(source_file)
)

# COMMAND ----------

#races_df.display()

# COMMAND ----------

#from pyspark.sql.functions import col,current_timestamp

#races_df = (
#    races_df.withColumn('current_timeStamp', current_timestamp())
#            .withColumn('source_file', col('_metadata.file_path'))
#)

races_df = ingest_metadata(races_df)

# COMMAND ----------

#races_df.display()

# COMMAND ----------

(
    races_df.write
    .format('delta')
    .mode('overwrite')
    #.option("mergeSchema", "true")
    .saveAsTable(table_name)
)

# COMMAND ----------

#%sql
#ALTER TABLE formula1.bronze.races SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name'); ALTER TABLE formula1.bronze.races DROP COLUMN current_timeStamp;

# COMMAND ----------

#spark.table(table_name).display()