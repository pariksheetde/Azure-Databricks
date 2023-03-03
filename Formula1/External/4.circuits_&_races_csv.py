# Databricks notebook source
# MAGIC %run "../Includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE EXTERNAL TABLE FOR CIRCUITS

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.circuits;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.circuits
# MAGIC (
# MAGIC circuitId integer,
# MAGIC circuitRef string,
# MAGIC name string,
# MAGIC location string,
# MAGIC country string,
# MAGIC lat double,
# MAGIC lng double,
# MAGIC alt double,
# MAGIC url string
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path "/mnt/adobeadls/raw/circuits.csv", header True)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.circuits;

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.races;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.races
# MAGIC (
# MAGIC race_id integer,
# MAGIC year integer,
# MAGIC round integer,
# MAGIC circuitid integer,
# MAGIC name string,
# MAGIC date date,
# MAGIC time string,
# MAGIC url string
# MAGIC )
# MAGIC using csv
# MAGIC options (path "/mnt/adobeadls/raw/races.csv", header True)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.races;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
