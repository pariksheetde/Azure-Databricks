-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### READ THE CUSTOMERS DATA FROM ADLS (Azure Data Lake Storage)

-- COMMAND ----------

SELECT * FROM json.`/mnt/adobeadls/dwanalytics/customers/processed/*`

-- COMMAND ----------

SELECT
input_file_name() as file_name,
count(*) AS cnt
FROM json.`/mnt/adobeadls/dwanalytics/customers/processed/*`
GROUP BY file_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE DELTA TABLE TO LOAD CUSTOMERS DATA

-- COMMAND ----------

DROP TABLE IF EXISTS dw_analytics.customers;
CREATE TABLE dw_analytics.customers
AS
SELECT * FROM json.`/mnt/adobeadls/dwanalytics/customers/processed/*`;

SELECT * FROM dw_analytics.customers;

-- COMMAND ----------

SELECT COUNT(*) AS count FROM dw_analytics.customers;

-- COMMAND ----------

DESCRIBE EXTENDED dw_analytics.customers;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

-- COMMAND ----------

DROP TABLE IF EXISTS dw_analytics.customers;
