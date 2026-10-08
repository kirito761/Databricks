-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS formula1.control
    MANAGED LOCATION 'abfss://formula1@databrickscoursedl1.dfs.core.windows.net/';

-- COMMAND ----------

SHOW SCHEMAS

-- COMMAND ----------

create table if not exists formula1.control.checkupdate (
     id INT,
    current_timestamp TIMESTAMP
)

-- COMMAND ----------

insert into formula1.control.checkupdate 
values (1, current_timestamp)

-- COMMAND ----------

delete from formula1.control.checkupdate 

-- COMMAND ----------

select * from formula1.control.checkupdate