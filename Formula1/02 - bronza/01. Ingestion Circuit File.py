# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "../00 - common/01- ingestion data"

# COMMAND ----------

# MAGIC %run "../00 - common/02-bronze helper"

# COMMAND ----------

source_file = f"{landing_folder_path}/circuits.csv"
table_name = f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------

source_file

# COMMAND ----------

table_name

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

circuit_schema = StructType([
    StructField('circuitId', StringType()),
    StructField('url', StringType()),
    StructField('circuitName', StringType()),
    StructField('lat', DoubleType()),
    StructField('long', DoubleType()),
    StructField('locality', StringType()),
    StructField('country', StringType())  
])

# COMMAND ----------

circuit_df = (
    spark.read
         .format('csv')
         .option('header', True)
         #.option('inferSchema', True)
         .schema(circuit_schema)
         .option('Mode','FAILFAST')
         .load(source_file)
)


# COMMAND ----------

circuit_df.display()

# COMMAND ----------

#from pyspark.sql.functions import current_timestamp,col

#circuit_df = (
#    circuit_df.withColumn('Currret_timeStamp', current_timestamp())
#              .withColumn('source_file', col('_metadata.file_path'))
#)

circuit_df = ingest_metadata(circuit_df)

# COMMAND ----------

circuit_df.display()

# COMMAND ----------

(
    circuit_df.write
            .format('delta')
            .mode('overwrite')
            .saveAsTable(table_name)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.bronze.circuits

# COMMAND ----------

spark.table(table_name).display()