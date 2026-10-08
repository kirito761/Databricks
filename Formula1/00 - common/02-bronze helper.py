# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql.functions import current_timestamp,col

def ingest_metadata(df):
    return(
        df.withColumn('Currret_timeStamp', current_timestamp())
          .withColumn('source_file', col('_metadata.file_path'))
    )