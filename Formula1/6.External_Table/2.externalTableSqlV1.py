# Databricks notebook source
# MAGIC %run "../9.Includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA FROM PRESENTATION LAYER

# COMMAND ----------

race_results_df = spark.read.parquet("/mnt/adobeadls/presentation/race_results")
display(race_results_df)
print(f"Number of records fetched {race_results_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE THE DATA TO EXTERNAL TABLE USING PYTHON

# COMMAND ----------

race_results_df.write.mode("overwrite").option("path",f"{presentation_path}/external/race_results_ext_python_v2").saveAsTable("f1_presentation.race_results_ext_python_v2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM f1_presentation.race_results_ext_python_v2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results_ext_python_v2 LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE THE DATA TO THE MANAGED TABLE USING SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.race_results_ext_sql_v1;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.race_results_ext_sql_v1
# MAGIC (
# MAGIC race_year INT,
# MAGIC race_name STRING,
# MAGIC race_date TIMESTAMP,
# MAGIC circuit_location STRING,
# MAGIC driver_name STRING,
# MAGIC driver_number INT,
# MAGIC driver_nationality STRING,
# MAGIC team STRING,
# MAGIC grid INT,
# MAGIC fastest_lap INT,
# MAGIC race_time STRING,
# MAGIC points INT,
# MAGIC position INT,
# MAGIC created_dt TIMESTAMP
# MAGIC )
# MAGIC USING CSV
# MAGIC LOCATION "/mnt/adobeadls/presentation/external/race_results_ext_sql_v1"

# COMMAND ----------

# %fs rm -r "/mnt/adobeadls/presentation/external/race_results_ext_sql_v1"

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE f1_presentation.race_results_ext_sql_v1;

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE f1_presentation.race_results_ext_python_v1;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_presentation.race_results_ext_sql_v1
# MAGIC SELECT * FROM f1_presentation.race_results_ext_python_v1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### VALIDATE EXTERNAL TABLE IS LOADED WITH ACCURATE ROW COUNT

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS cnt FROM f1_presentation.race_results_ext_sql_v1;

# COMMAND ----------

validate_ext_sql_df = spark.read \
.option("header", False) \
.csv("/mnt/adobeadls/presentation/external/race_results_ext_sql_v1") 
        
display(validate_ext_sql_df)
print(validate_ext_sql_df.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED f1_presentation.race_results_ext_sql_v1;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
