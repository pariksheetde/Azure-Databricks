-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### CONSTRUCTOR'S STANDING

-- COMMAND ----------

SELECT
races.race_year,
constructors.constructor_ref,
SUM(results.points) AS sum_points,
COUNT(CASE WHEN results.position = 1 THEN 'True' Else 'False' END) AS win
FROM
dw_analytics.results_parquet results JOIN dw_analytics.races_parquet races
ON races.race_id = results.race_id JOIN dw_analytics.drivers_json drivers
ON drivers.driverid = results.driver_id JOIN dw_analytics.constructors_parquet constructors
ON constructors.constructor_id = results.constructor_id
GROUP BY
races.race_year,
constructors.constructor_ref,
results.position
HAVING sum_points > 100
ORDER BY races.race_year;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("SUCCESSFULLY EXECUTED")
