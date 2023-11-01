-- Databricks notebook source
-- MAGIC %sql
-- MAGIC MERGE INTO streaming.customers tgt
-- MAGIC USING customers_streaming_temp_vw src
-- MAGIC ON tgt.customer_id = src.customer_id
-- MAGIC WHEN MATCHED AND tgt.email IS NULL AND src.email IS NOT NULL THEN
-- MAGIC   UPDATE SET tgt.email = src.email, tgt.updated = src.updated
-- MAGIC WHEN NOT MATCHED THEN
-- MAGIC   INSERT *;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT COUNT(*) AS CNT FROM streaming.customers;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM streaming.customers;
