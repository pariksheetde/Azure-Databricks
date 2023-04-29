# Databricks notebook source
# MAGIC %run "../Includes/config"

# COMMAND ----------

# MAGIC %python
# MAGIC race_results_df = spark.read.parquet(f"{presentation_path}/race_results")
# MAGIC display(race_results_df)
# MAGIC race_results_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.race_results_managed_sql_v1;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.race_results_managed_sql_v1
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

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_presentation.race_results_managed_sql_v1
# MAGIC SELECT * FROM f1_presentation.race_results_managed_python_v1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.race_results_managed_sql_v1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED f1_presentation.race_results_managed_sql_v1;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
