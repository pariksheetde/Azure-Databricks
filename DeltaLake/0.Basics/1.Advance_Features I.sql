-- Databricks notebook source
DESC HISTORY delta.locations;

-- COMMAND ----------

SELECT * FROM delta.locations TIMESTAMP AS OF '2023-03-05T12:11:32.000+0000';

-- COMMAND ----------

SELECT * FROM delta.locations VERSION AS OF 1;

-- COMMAND ----------

SELECT * FROM delta.locations@V2;

-- COMMAND ----------

-- RESTORE TABLE delta.locations TO VERSION AS OF 2

-- COMMAND ----------

DESC EXTENDED delta.locations;
