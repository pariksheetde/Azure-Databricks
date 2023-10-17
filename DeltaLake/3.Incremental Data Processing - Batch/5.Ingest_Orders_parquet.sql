-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### READ THE ORDERS DATA FROM ADLS (Azure Data Lake Storage)

-- COMMAND ----------

SELECT * FROM parquet.`/mnt/adobeadls/dwanalytics/orders/processed/*.parquet`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE TEMP TABLE TO LOAD DATA FROM ADLS (Azure Data Lake Storage)

-- COMMAND ----------

CREATE OR REPLACE TABLE dw_analytics.orders
AS
SELECT * FROM parquet.`/mnt/adobeadls/dwanalytics/orders/processed/*`;

SELECT * FROM dw_analytics.orders ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### VALIDATE THE RECORD COUNT

-- COMMAND ----------

SELECT count(*) AS CNT FROM dw_analytics.orders;

-- COMMAND ----------

DESCRIBE EXTENDED dw_analytics.orders;

-- COMMAND ----------

-- DO NOT DELETE THIS CELL

-- INSERT OVERWRITE dw_analytics.orders
-- SELECT * FROM parquet.`/mnt/adobeadls/dwanalytics/orders/processed/*`;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

-- COMMAND ----------

DROP TABLE dw_analytics.orders;
