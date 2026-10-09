-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS formula1.control
    MANAGED LOCATION 'abfss://formula1@databrickscourcestorage1.dfs.core.windows.net/';

-- COMMAND ----------

--select current_catalog()

-- COMMAND ----------

--USE CATALOG formula1;

-- COMMAND ----------

--SHOW SCHEMAS

-- COMMAND ----------

create table if not exists formula1.control.checkupdate (
     id INT,
    current_timestamp TIMESTAMP
)

-- COMMAND ----------

--DESCRIBE STORAGE CREDENTIAL databrickscoursesc;

-- COMMAND ----------

--select * from formula1.control.checkupdate;

-- COMMAND ----------

insert into formula1.control.checkupdate 
values (1, current_timestamp)

-- COMMAND ----------

--delete from formula1.control.checkupdate 

-- COMMAND ----------

--select * from formula1.control.checkupdate