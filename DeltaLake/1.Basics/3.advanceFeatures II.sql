-- Databricks notebook source
OPTIMIZE delta.locations
ZORDER BY loc_id;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
