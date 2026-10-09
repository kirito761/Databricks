-- Databricks notebook source
-- MAGIC %fs ls 'abfss://formula1@databrickscourcestorage1.dfs.core.windows.net/'

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_course_ext_dl1_formula1_1
    URL 'abfss://formula1@databrickscourcestorage1.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL databrickscoursesc)
    COMMENT 'DL1 formula1 location 1';

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

CREATE CATALOG  IF NOT EXISTS  formula1
   MANAGED LOCATION 'abfss://formula1@databrickscourcestorage1.dfs.core.windows.net/'
   COMMENT 'catalog for formula1';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1.landing;
CREATE SCHEMA IF NOT EXISTS formula1.gold
    MANAGED LOCATION 'abfss://formula1@databrickscourcestorage1.dfs.core.windows.net/gold';
CREATE SCHEMA IF NOT EXISTS formula1.silver
    MANAGED LOCATION 'abfss://formula1@databrickscourcestorage1.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS formula1.bronze
    MANAGED LOCATION 'abfss://formula1@databrickscourcestorage1.dfs.core.windows.net/bronze';

-- COMMAND ----------

SHOW SCHEMAS

-- COMMAND ----------

select CURRENT_CATALOG()

-- COMMAND ----------

USE CATALOG formula1;

-- COMMAND ----------

CREATE EXTERNAL VOLUME formula1.landing.files
    LOCATION 'abfss://formula1@databrickscourcestorage1.dfs.core.windows.net/landing'

-- COMMAND ----------

--DROP CATALOG formula1 CASCADE;

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1/landing/files