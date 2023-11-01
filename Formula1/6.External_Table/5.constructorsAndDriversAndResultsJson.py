# Databricks notebook source
# MAGIC %md
# MAGIC #### CREATE f1_presentation.constructors EXTERNAL TABLE
# MAGIC - Single Line JSON
# MAGIC - Simple structure

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.constructors;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.constructors
# MAGIC (
# MAGIC constructorId INT,
# MAGIC constructorRef string,
# MAGIC name string,
# MAGIC nationality string,
# MAGIC url string
# MAGIC )
# MAGIC using json
# MAGIC options (path "/mnt/adobeadls/raw/constructors.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructors;

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE f1_presentation.drivers EXTERNAL TABLE
# MAGIC - Single Line JSON
# MAGIC - Simple Structure

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.drivers;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.drivers
# MAGIC (
# MAGIC code STRING,
# MAGIC dob DATE,
# MAGIC driverId INT,
# MAGIC driverRef STRING,
# MAGIC name STRUCT<forename: STRING, surname: STRING>,
# MAGIC nationality STRING,
# MAGIC number INT,
# MAGIC url string
# MAGIC )
# MAGIC using json
# MAGIC options (path "/mnt/adobeadls/raw/drivers.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC code, 
# MAGIC dob, driverId, driverRef, 
# MAGIC name.forename, 
# MAGIC name.surname  
# MAGIC FROM f1_presentation.drivers;

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE f1_presentation.results EXTERNAL TABLE
# MAGIC - Single Line JSON
# MAGIC - Simple Structure

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.results;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.results
# MAGIC (
# MAGIC constructorId integer,
# MAGIC driverId integer,
# MAGIC fastestLap integer,
# MAGIC fastestLapSpeed float,
# MAGIC fastestLapTime string,
# MAGIC grid integer,
# MAGIC laps integer,
# MAGIC milliseconds integer,
# MAGIC number integer,
# MAGIC points float,
# MAGIC position integer,
# MAGIC positionOrder integer,
# MAGIC positionText string,
# MAGIC raceId integer,
# MAGIC rank integer,
# MAGIC resultId integer,
# MAGIC statusId string,
# MAGIC time string
# MAGIC )
# MAGIC using json
# MAGIC options (path "/mnt/adobeadls/raw/results.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.results;

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE f1_presentation.pit_stops EXTERNAL TABLE
# MAGIC - Multi-Line JSON
# MAGIC - Simple Structure

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.pit_stops;
# MAGIC CREATE TABLE f1_presentation.pit_stops
# MAGIC (
# MAGIC driverId integer,
# MAGIC duration string,
# MAGIC lap integer,
# MAGIC milliseconds integer,
# MAGIC raceId integer,
# MAGIC stop string,
# MAGIC time string
# MAGIC )
# MAGIC using json
# MAGIC options (path "/mnt/adobeadls/raw/pit_stops.json", multiLine True)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.pit_stops;

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE f1_presentation.lap_times EXTERNAL TABLE
# MAGIC - CSV files
# MAGIC - Multiple files

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.lap_times;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.lap_times
# MAGIC (
# MAGIC raceId integer,
# MAGIC driverId integer,
# MAGIC lap integer,
# MAGIC position integer,
# MAGIC time string,
# MAGIC milliseconds integer
# MAGIC )
# MAGIC using csv
# MAGIC options (path "/mnt/adobeadls/raw/lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.lap_times;

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE f1_presentation.qualifying EXTERNAL TABLE
# MAGIC - Multi-Line JSON file
# MAGIC - Multiple files

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.qualifying;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.qualifying
# MAGIC (
# MAGIC constructorId integer,
# MAGIC driverId integer,
# MAGIC number integer,
# MAGIC position integer,
# MAGIC q1 string,
# MAGIC q2 string,
# MAGIC q3 string,
# MAGIC qualifyId integer,
# MAGIC raceId integer
# MAGIC )
# MAGIC using json
# MAGIC options (path "/mnt/adobeadls/raw/qualifying/", multiLine True)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.qualifying;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
