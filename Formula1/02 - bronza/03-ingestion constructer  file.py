# Databricks notebook source
# MAGIC %run "../00 - common/01- ingestion data"

# COMMAND ----------

# MAGIC %run "../00 - common/02-bronze helper"

# COMMAND ----------

source_file = f'{landing_folder_path}/constructors.json'
table_name = f'{catalog_name}.{bronze_schema}.constructors'

# COMMAND ----------

constructors_schema = """
constructorId STRING,
name STRING,
nationality STRING,
url STRING
"""

# COMMAND ----------

constructors_df = (
    spark.read
        .format('json')
        .schema(constructors_schema)
        .option('mode','FAILFAST')
        .load(source_file)
)

# COMMAND ----------

constructors_df = ingest_metadata(constructors_df)

# COMMAND ----------

constructors_df.display()

# COMMAND ----------

(
    constructors_df.write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(table_name)
)

# COMMAND ----------

spark.table(table_name).display()