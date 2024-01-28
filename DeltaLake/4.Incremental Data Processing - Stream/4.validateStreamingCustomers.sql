-- Databricks notebook source
MERGE INTO streaming.customers tgt
USING customers_streaming_temp_vw src
ON tgt.customer_id = src.customer_id
WHEN MATCHED AND tgt.email IS NULL AND src.email IS NOT NULL THEN
  UPDATE SET tgt.email = src.email, tgt.updated = src.updated
WHEN NOT MATCHED THEN
  INSERT *;

-- COMMAND ----------

SELECT COUNT(*) AS CNT FROM streaming.customers;

-- COMMAND ----------

SELECT * FROM streaming.customers;
