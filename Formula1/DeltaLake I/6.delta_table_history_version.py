# Databricks notebook source
# MAGIC %sql
# MAGIC DESC HISTORY f1_delta.driver_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.driver_merge VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.driver_merge TIMESTAMP AS OF "2022-07-06T12:52:17.000+0000";
