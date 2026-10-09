# Databricks notebook source
# MAGIC %run "../00 - common/01- ingestion data"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

constructors_df = (
    spark.table(bronze_table)
)

# COMMAND ----------

#constructors_df.display()

# COMMAND ----------

from pyspark.sql.functions import col

constructors_selected_df=(
    constructors_df.drop("url")
)

# COMMAND ----------

constructors_renamed_df = (
    constructors_selected_df.withColumnsRenamed({
        "constructorId" : "constructor_id",
        "name" : "constructor_name"
    })
)

# COMMAND ----------

#constructors_renamed_df.display()

# COMMAND ----------

constructors_validated_df = constructors_renamed_df.filter("constructor_id is not null")

# COMMAND ----------

from pyspark.sql.functions import initcap

constructors_final_df = (
    constructors_validated_df.withColumn("nationality",initcap("nationality")))

# COMMAND ----------

(
    constructors_final_df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(silver_table)
)

# COMMAND ----------

#spark.table(silver_table).display()