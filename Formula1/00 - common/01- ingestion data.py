# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
catalog_name = 'formula1'
bronze_schema = 'bronze'
silver_schema = 'silver'
gold_schema = 'gold'

# COMMAND ----------

landing_folder_path = '/Volumes/formula1/landing/files/'

# COMMAND ----------

# MAGIC %fs ls dbfs:/Volumes/

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_metastore()