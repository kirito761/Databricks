# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run "../00 - common/01- ingestion data"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

drivers_df = (
    spark.table(bronze_table)
)

# COMMAND ----------

#drivers_df.display()

# COMMAND ----------

from pyspark.sql.functions import col

drivers_selected_df=(
    drivers_df.drop("url")
)

# COMMAND ----------

drivers_renamed_df = (
    drivers_selected_df.withColumnsRenamed({
        "driverId" : "driver_id",
        "dateOfBirth" : "date_of_birth"
    })
)

# COMMAND ----------

#drivers_renamed_df.display()

# COMMAND ----------

from pyspark.sql.functions import col,lit,concat_ws,initcap

drivers_concatenated_df = (
    drivers_renamed_df.withColumn("driver_name",initcap(concat_ws(" ",col("name.givenName"),col("name.familyName"))))
            .drop("name")
)


# COMMAND ----------

drivers_validated_df = drivers_concatenated_df.filter("driver_id is not null")

# COMMAND ----------

from pyspark.sql.functions import initcap

drivers_final_df = (
    drivers_validated_df.withColumn("nationality",initcap("nationality")))

# COMMAND ----------

(
    drivers_final_df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(silver_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC to remove column
# MAGIC ALTER TABLE formula1.silver.drivers SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name'); ALTER TABLE formula1.silver.drivers DROP COLUMN name;

# COMMAND ----------

#spark.table(silver_table).display()