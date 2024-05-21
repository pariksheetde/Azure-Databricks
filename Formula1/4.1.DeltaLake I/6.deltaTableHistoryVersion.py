# Databricks notebook source
# MAGIC %sql
# MAGIC DESC HISTORY f1_delta.driver_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.driver_merge VERSION AS OF 2;

# COMMAND ----------

dbutils.notebook.exit("SUCCESSFULLY EXECUTED")
