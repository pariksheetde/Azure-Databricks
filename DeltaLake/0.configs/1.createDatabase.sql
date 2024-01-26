-- Databricks notebook source
DROP DATABASE IF EXISTS dw_analytics CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS dw_analytics;

-- COMMAND ----------

DROP DATABASE IF EXISTS streaming CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS streaming;

-- COMMAND ----------

DROP DATABASE IF EXISTS bronze CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS bronze;

-- COMMAND ----------

DROP DATABASE IF EXISTS silver CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS silver;

-- COMMAND ----------

DROP DATABASE IF EXISTS gold CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS gold;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
