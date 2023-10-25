# Databricks notebook source
# MAGIC %run "../9.Includes/config"

# COMMAND ----------

# MAGIC %python
# MAGIC race_results_df = spark.read.parquet(f"{presentation_path}/race_results")
# MAGIC display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE EXTERNAL TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.race_results_ext_sql_v2;
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS f1_presentation.race_results_ext_sql_v2
# MAGIC (
# MAGIC race_year INT,
# MAGIC race_name STRING,
# MAGIC race_date timestamp,
# MAGIC circuit_location string,
# MAGIC driver_name string,
# MAGIC driver_number integer,
# MAGIC driver_nationality string,
# MAGIC team string,
# MAGIC grid integer,
# MAGIC fastest_lap integer,
# MAGIC race_time string,
# MAGIC points float,
# MAGIC position integer,
# MAGIC created_dt timestamp
# MAGIC )
# MAGIC USING JSON
# MAGIC LOCATION "/mnt/adobeadls/presentation/external/race_results_ext_sql_v2"

# COMMAND ----------

# %fs rm -r "/mnt/presentation/external/race_results_ext_sql_v2"

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_presentation.race_results_ext_sql_v2
# MAGIC SELECT * FROM f1_presentation.race_results_ext_python_v2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM f1_presentation.race_results_ext_sql_v2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.race_results_ext_python_v2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED f1_presentation.race_results_ext_sql_v2;

# COMMAND ----------

# MAGIC %md
# MAGIC #### VALIDATE THAT CORRECT DATA IN JSON FORMAT HAS BEEN WRITTEN TO EXTERNAL TABLE

# COMMAND ----------

race_results_ext_sql_df = spark.read.json("/mnt/adobeadls/presentation/external/race_results_ext_sql_v2")
display(race_results_ext_sql_df)
print(f"Number of Records Fetched {race_results_ext_sql_df.count()}")

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
