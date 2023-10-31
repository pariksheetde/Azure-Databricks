-- Databricks notebook source
DESC HISTORY delta.locations;

-- COMMAND ----------

SELECT * FROM delta.locations VERSION AS OF 1;

-- COMMAND ----------

SELECT * FROM delta.locations@V1;

-- COMMAND ----------

-- RESTORE TABLE delta.locations TO VERSION AS OF 2

-- COMMAND ----------

DESC EXTENDED delta.locations;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
