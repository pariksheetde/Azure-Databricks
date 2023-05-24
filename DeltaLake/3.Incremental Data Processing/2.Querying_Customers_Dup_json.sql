-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### READ THE CUSTOMERS DATA FROM ADLS (Azure Data Lake Storage)

-- COMMAND ----------

SELECT * FROM json.`/mnt/adobeadls/dwanalytics/customers/*`

-- COMMAND ----------

SELECT
count(*) AS cnt
FROM json.`/mnt/adobeadls/dwanalytics/customers/*`;

-- COMMAND ----------

SELECT
input_file_name() as source_file
,count(*) AS cnt
FROM json.`/mnt/adobeadls/dwanalytics/customers/*`
GROUP BY source_file
ORDER BY source_file ASC;

-- COMMAND ----------

DROP TABLE IF EXISTS dw_analytics.customers;
CREATE TABLE dw_analytics.customers
AS
SELECT * FROM json.`/mnt/adobeadls/dwanalytics/customers/*`;

-- COMMAND ----------

SELECT COUNT(*) AS CNT FROM dw_analytics.customers;

-- COMMAND ----------

SELECT
input_file_name() AS source_file
,count(*) AS cnt
FROM dw_analytics.customers
GROUP BY source_file;

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_temp_vw
AS SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;

MERGE INTO dw_analytics.customers tgt
USING customers_temp_vw src
ON tgt.customer_id = src.customer_id
WHEN MATCHED AND tgt.email IS NULL AND src.email IS NOT NULL THEN
  UPDATE SET tgt.email = src.email, tgt.updated = src.updated
WHEN NOT MATCHED THEN
  INSERT *;

-- COMMAND ----------

SELECT count(*) AS cnt FROM dw_analytics.customers;

-- COMMAND ----------

DESC EXTENDED dw_analytics.customers;

-- COMMAND ----------

SELECT 
customer_id as cust_id
,email
,profile:first_name as first_name
,profile:last_name as last_name
,profile:first_name||' '||profile:last_name as full_name
,profile:gender
,profile:address:street
,profile:address:city
,profile:address:country
 FROM dw_analytics.customers
 ORDER BY 1 ASC;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
