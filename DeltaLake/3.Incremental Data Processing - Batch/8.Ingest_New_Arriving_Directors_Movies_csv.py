# Databricks notebook source
# MAGIC %md
# MAGIC ####1. NEW FILES HAVE BEEN LOADED IN ADLS. WRITE A QUERY TO VIEW ONLY THE NEW ARRIVING FILES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`/mnt/adobeadls/dwanalytics/movies/landing_zone/*`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW directors_temp_vw
# MAGIC (
# MAGIC     ID INT,
# MAGIC     F_Name STRING,
# MAGIC     L_Name STRING,
# MAGIC     Year INT,
# MAGIC     MovieName STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS
# MAGIC (
# MAGIC   path = "/mnt/adobeadls/dwanalytics/movies/landing_zone/*",
# MAGIC   header = "true",
# MAGIC   sep = ","
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM directors_temp_vw ORDER BY 1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### CHECK TO SEE DUPLICATE RECORDS HAS NOT BEEN LOADED. AUTOLOADER GUARANTEES THAT THE DATA BEING LOADED IS SKIPPED

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM directors_temp_vw ORDER BY 1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### LOADING THE DATA TO THE DW directors TABLE TO ENSURE THE INCREMENTAL DATA HAS BEEN LOADED TO THE DW TARGET TABLE (DESTINATION TABLE)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dw_analytics.directors_movies AS tgt
# MAGIC USING directors_temp_vw src
# MAGIC ON tgt.id = src.id
# MAGIC   WHEN MATCHED THEN UPDATE SET tgt.id = src.id, tgt.f_name = src.f_name, tgt.l_name = src.l_name, tgt.year = src.year, tgt.moviename = src.moviename 
# MAGIC   WHEN NOT MATCHED THEN INSERT *
# MAGIC -- INSERT INTO dw_analytics.directors_movies (id, f_name, l_name, year, moviename)
# MAGIC -- SELECT id, f_name, l_name, year, moviename FROM directors_temp_vw WHERE id not in (SELECT id FROM dw_analytics.directors_movies);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC count(moviename) as cnt,
# MAGIC f_name||' '||l_name as directors
# MAGIC FROM dw_analytics.directors_movies
# MAGIC GROUP BY directors;

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dw_analytics.books_staging;
