# Databricks notebook source
# MAGIC %md
# MAGIC #### QUERY movies table from Database

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dw_analytics.directors_movies;

# COMMAND ----------

spark.readStream \
    .table("dw_analytics.directors_movies") \
    .createOrReplaceTempView("directors_movies_streaming_temp_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM directors_movies_streaming_temp_vw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC count(moviename) as cnt,
# MAGIC f_name||' '||l_name as directors
# MAGIC FROM directors_movies_streaming_temp_vw
# MAGIC GROUP BY directors;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW directors_movie_aggregation_tmp_vw
# MAGIC AS
# MAGIC (
# MAGIC SELECT 
# MAGIC count(moviename) as cnt,
# MAGIC f_name||' '||l_name as directors
# MAGIC FROM directors_movies_streaming_temp_vw
# MAGIC GROUP BY directors
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM directors_movie_aggregation_tmp_vw;

# COMMAND ----------

spark.table("directors_movie_aggregation_tmp_vw") \
    .writeStream \
    .trigger(processingTime = '1 seconds') \
    .outputMode('complete') \
    .option("checkpointLocation", "/mnt/adobeadls/dwanalytics/movies/checkpoint/directors_movie_aggregation") \
    .table("dw_analytics.directors_movie_aggregation")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dw_analytics.directors_movie_aggregation;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dw_analytics.directors_movie_aggregation;
