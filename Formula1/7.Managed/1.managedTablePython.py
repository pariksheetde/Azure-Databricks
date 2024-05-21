# Databricks notebook source
# MAGIC %run "../9.Includes/1.config"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_path}/race_results")
display(race_results_df)

# COMMAND ----------

race_results_df.write.mode("overWrite").format("parquet").saveAsTable("f1_presentation.race_results_managed_python_v1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results_managed_python_v1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.race_results_managed_python_v1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED f1_presentation.race_results_managed_python_v1;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
