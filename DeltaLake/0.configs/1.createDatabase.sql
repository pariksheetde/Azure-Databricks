-- Databricks notebook source
-- MAGIC %sql
-- MAGIC DROP DATABASE IF EXISTS dw_analytics CASCADE;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE DATABASE IF NOT EXISTS dw_analytics;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP DATABASE IF EXISTS streaming CASCADE;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE DATABASE IF NOT EXISTS streaming;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP DATABASE IF EXISTS bronze CASCADE;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE DATABASE IF NOT EXISTS bronze;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP DATABASE IF EXISTS silver CASCADE;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE DATABASE IF NOT EXISTS silver;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
