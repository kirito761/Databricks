-- Databricks notebook source
select * from formula1.gold.fact_session_result;

-- COMMAND ----------

select * from formula1.gold.fact_result_table;

-- COMMAND ----------

Create or replace view formula1.gold.vw_constructor_standings as
with standing_details as(
select f.season,
    f.constructor_id,
    c.constructor_name,
    c.nationality,    
    count(*) as race_starts,
    sum(f.points) as total_points,
    count_if(f.is_win) as number_of_wins,
    count_if(f.is_podium) as number_of_podiums
from formula1.gold.fact_session_result f
join
formula1.gold.dim_constructor c on
f.constructor_id = c.constructor_id
group by
f.season,
    f.constructor_id,
    c.constructor_name,
    c.nationality)
select season,
constructor_id,
constructor_name,
RANK() OVER(PARTITION By season ORDER BY total_points DESC,number_of_wins DESC) as Standing,
nationality,
race_starts,
total_points,
number_of_wins,
number_of_podiums
 from standing_details;


-- COMMAND ----------

select * from formula1.gold.vw_constructor_standings where Standing == 1;