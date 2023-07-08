-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### New Files have been loaded in ADLS. Write a query to view only the new arriving files

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

CREATE OR REPLACE TEMP VIEW directors_movies_staging_agg_temp_vw
AS
SELECT * FROM directors_movies_staging_temp_vw WHERE ID NOT IN (SELECT ID FROM dw_analytics.directors_movies);

-- COMMAND ----------

SELECT * FROM directors_movies_staging_agg_temp_vw;

-- COMMAND ----------

MERGE INTO dw_analytics.directors_movies tgt
USING directors_movies_staging_agg_temp_vw src
ON tgt.id = src.id
  WHEN MATCHED THEN UPDATE SET tgt.f_name = src.f_name, tgt.l_name = src.l_name, tgt.year = src.year, tgt.moviename = src.moviename 
  WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

-- COMMAND ----------

SELECT * FROM dw_analytics.directors_movies ORDER BY 1;
