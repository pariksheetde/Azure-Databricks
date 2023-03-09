-- Databricks notebook source
SELECT
circuits.circuit_id,
circuits.name,
circuits.location,
circuits.country,
-- select columns from delta.races
races.race_id,
races.round,
races.name as race_name,
races.race_year,
-- select columns from delta.results
results.fastest_lap,
results.constructor_id,
-- select columns from delta.constructors
constructors.name,
constructors.nationality,
-- select columns from delta.drivers
drivers.code,
drivers.dob,
drivers.name.forename,
drivers.name.surname,
drivers.nationality,
-- select columns from delta.pit_stops
pit_stops.lap
FROM
delta.circuits_parquet circuits JOIN delta.races_parquet races
ON circuits.circuit_id = races.circuit_id JOIN delta.results_parquet results
ON races.race_id = results.race_id JOIN delta.constructors_parquet constructors
ON constructors.constructor_id = results.constructor_id JOIN delta.drivers_json drivers
ON drivers.driverid = results.driver_id JOIN delta.pit_stops_parquet pit_stops
ON pit_stops.driver_id = drivers.driverid AND pit_stops.race_id = races.race_id;
