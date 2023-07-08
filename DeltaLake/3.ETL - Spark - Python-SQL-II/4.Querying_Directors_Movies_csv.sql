-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### READ THE MOVIES DATA FROM ADLS (Azure Data Lake Storage)

-- COMMAND ----------

SELECT * FROM csv.`/mnt/adobeadls/dwanalytics/movies/*.csv`

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW directors_movies_staging_temp_vw
(
  id INT,
  f_name STRING,
  l_name STRING,
  year INT,
  moviename STRING
  )
USING CSV
OPTIONS
(
  path = "/mnt/adobeadls/dwanalytics/movies/*.csv",
  header = "true"
);

SELECT * FROM directors_movies_staging_temp_vw;

-- COMMAND ----------

SELECT COUNT(*) AS cnt FROM directors_movies_staging_temp_vw;

-- COMMAND ----------

CREATE OR REPLACE TABLE dw_analytics.directors_movies
AS
SELECT * FROM directors_movies_staging_temp_vw;

SELECT * FROM dw_analytics.directors_movies;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
