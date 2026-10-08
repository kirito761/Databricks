-- Databricks notebook source
select season,driver_id,sum(points) from formula1.gold.fact_session_result where driver_id = 'norris' group by season,driver_id;

-- COMMAND ----------

CREATE OR REPLACE VIEW formula1.gold.vw_driver_standings as
WITH total_data_view as
(select f.season,
        f.driver_id,
        d.driver_name,
        d.nationality,
        count(*) as race_starts,
        sum(points) as total_points,
        count_if(is_win) as number_of_wins,
        count_if(is_podium) as number_of_podiums
from formula1.gold.fact_session_result f join 
formula1.gold.dim_driver d 
on f.driver_id = d.driver_id
group by f.season,
        f.driver_id,
        d.driver_name,
        d.nationality)
select 
season,
driver_id,
driver_name,
nationality,
race_starts,
total_points,
number_of_wins,
number_of_podiums,
RANK() OVER(PARTITION BY season ORDER BY total_points DESC,number_of_wins DESC) as standing
from total_data_view;

-- COMMAND ----------

select * from formula1.gold.vw_driver_standings where season = 2025;