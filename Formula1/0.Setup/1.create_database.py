# Databricks notebook source
# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_presentation CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_presentation;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_etl CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_etl;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_incremental CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_incremental
# MAGIC LOCATION "/mnt/adobeadls/incremental";

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_delta CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_delta
# MAGIC LOCATION "/mnt/adobeadls/deltalake";

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_processed;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS dw_analytics CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS dw_analytics;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS streaming CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS streaming;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
