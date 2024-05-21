# Databricks notebook source
# MAGIC %run "../9.Includes/1.config"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_path}/race_results")
display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### LOAD THE DATA INTO EXTERNAL TABLE

# COMMAND ----------

race_results_df.write.mode("overWrite").format("json").option("path", f"{presentation_path}/external/race_results_ext_python_v1").saveAsTable("f1_presentation.race_results_ext_python_v1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results_ext_python_v1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM f1_presentation.race_results_ext_python_v1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED f1_presentation.race_results_ext_python_v1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### VALIDATE THAT CORRECT DATA IN JSON FORMAT HAS BEEN WRITTEN TO EXTERNAL TABLE

# COMMAND ----------

validate_df = spark.read.json(f"{presentation_path}/external/race_results_ext_python_v1")
display(validate_df)
validate_df.count()

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
