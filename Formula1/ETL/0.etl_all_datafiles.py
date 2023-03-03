# Databricks notebook source
dbutils.notebook.run("1.etl_circuits_datafile", 600)

# COMMAND ----------

dbutils.notebook.run("2.etl_races_datafile", 600)

# COMMAND ----------

dbutils.notebook.run("3.etl_constructors_datafile", 600)

# COMMAND ----------

dbutils.notebook.run("4.etl_drivers_datafile", 600)

# COMMAND ----------

dbutils.notebook.run("5.etl_results_datafile", 1200)

# COMMAND ----------

dbutils.notebook.run("6.etl_pitstops_datafile", 600)

# COMMAND ----------

dbutils.notebook.run("7.etl_lap_times_datafile", 600)

# COMMAND ----------

dbutils.notebook.run("8.etl_qualifying_datafile", 600)
