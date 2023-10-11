-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### WRITE TRANSFORMATION QUERY

-- COMMAND ----------

SELECT
circuits.circuit_id,
circuits.name,
circuits.location,
circuits.country,
-- select columns from dw_analytics.races
races.race_id,
races.round,
races.name as race_name,
races.race_year,
-- select columns from dw_analytics.results
results.fastest_lap,
results.constructor_id,
-- select columns from dw_analytics.constructors
constructors.name,
constructors.nationality,
-- select columns from dw_analytics.drivers
drivers.code,
drivers.dob,
drivers.name.forename,
drivers.name.surname,
drivers.nationality,
-- select columns from dw_analytics.pit_stops
pit_stops.lap
FROM
dw_analytics.circuits_parquet circuits JOIN dw_analytics.races_parquet races
ON circuits.circuit_id = races.circuit_id JOIN dw_analytics.results_parquet results
ON races.race_id = results.race_id JOIN dw_analytics.constructors_parquet constructors
ON constructors.constructor_id = results.constructor_id JOIN dw_analytics.drivers_json drivers
ON drivers.driverid = results.driver_id JOIN dw_analytics.pit_stops_parquet pit_stops
ON pit_stops.driver_id = drivers.driverid AND pit_stops.race_id = races.race_id;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
