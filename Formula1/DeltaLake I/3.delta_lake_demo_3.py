# Databricks notebook source
# MAGIC %md
# MAGIC - History & Versioning
# MAGIC - Time Travel
# MAGIC - Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_delta.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.drivers_merge VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.drivers_merge TIMESTAMP AS OF '2022-12-26T06:22:56.000+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_delta.drivers_txn;
# MAGIC CREATE TABLE IF NOT EXISTS f1_delta.drivers_txn
# MAGIC (
# MAGIC driverid INT,
# MAGIC dob STRING,
# MAGIC firstname STRING,
# MAGIC lastname STRING,
# MAGIC created_dt DATE,
# MAGIC updated_dt DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_delta.drivers_txn
