# Databricks notebook source
# MAGIC %md
# MAGIC ###1. NEW FILES HAVE BEEN LOADED IN ADLS. WRITE A QUERY TO VIEW ONLY THE NEW ARRIVING FILES
# MAGIC ###2. AUTOLOADER IS USED TO LOAD THE INCREMENTAL RECORDS

# COMMAND ----------

spark.readStream \
     .format("cloudFiles") \
     .option("cloudFiles.format", "csv") \
     .option("cloudFiles.schemaLocation", "/mnt/adobeadls/dwanalytics/movies/checkpoint/landing_zone") \
     .load("/mnt/adobeadls/dwanalytics/movies/landing_zone/") \
.writeStream \
     .option("checkpointLocation", "/mnt/adobeadls/dwanalytics/movies/checkpoint/landing_zone") \
     .table("dw_analytics.books_staging")

# COMMAND ----------

# MAGIC %md
# MAGIC #### CHECK TO SEE DUPLICATE RECORDS HAS NOT BEEN LOADED. AUTOLOADER GUARANTEES THAT THE DATA BEING LOADED IS SKIPPED

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dw_analytics.books_staging ORDER BY 1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### LOADING THE DATA TO THE DW directors TABLE TO ENSURE THE INCREMENTAL DATA HAS BEEN LOADED TO THE DW TARGET TABLE (DESTINATION TABLE)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dw_analytics.directors_movies (id, f_name, l_name, year, moviename)
# MAGIC SELECT id, f_name, l_name, year, moviename FROM dw_analytics.books_staging WHERE id not in (SELECT id FROM dw_analytics.directors_movies);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC count(moviename) as cnt,
# MAGIC f_name||' '||l_name as directors
# MAGIC FROM dw_analytics.directors_movies
# MAGIC GROUP BY directors;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW dw_analytics.directors_movie_aggregation_vw
# MAGIC AS
# MAGIC (
# MAGIC SELECT 
# MAGIC count(moviename) as cnt,
# MAGIC f_name||' '||l_name as directors
# MAGIC FROM dw_analytics.directors_movies
# MAGIC GROUP BY directors
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dw_analytics.directors_movie_aggregation_vw;

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS dw_analytics.directors_movie_aggregation_vw;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dw_analytics.books_staging;
