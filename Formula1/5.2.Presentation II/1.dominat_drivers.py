# Databricks notebook source
# MAGIC %md
# MAGIC ##### MOST DOMINANT DRIVERS IN FORMULA 1

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.dominant_drivers;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.dominant_drivers
# MAGIC (
# MAGIC driver_name STRING,
# MAGIC total_races INT,
# MAGIC total_points INT,
# MAGIC avg_points INT,
# MAGIC rank INT
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_presentation.dominant_drivers
# MAGIC SELECT
# MAGIC driver_name
# MAGIC ,total_races
# MAGIC ,total_points
# MAGIC ,avg_points
# MAGIC ,RANK() OVER (ORDER BY avg_points DESC) as rank
# MAGIC FROM 
# MAGIC (
# MAGIC   SELECT 
# MAGIC   driver_name,
# MAGIC   count(1) as total_races,
# MAGIC   sum(calculated_points) as total_points,
# MAGIC   AVG(calculated_points) as avg_points
# MAGIC   FROM f1_presentation.calculated_race_results_vw
# MAGIC   GROUP BY driver_name
# MAGIC   ORDER BY avg_points DESC) temp
# MAGIC WHERE total_races >= 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS CNT FROM f1_presentation.dominant_drivers;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### MOST DOMINANT DRIVERS IN LAST DECADE IN FORMULA 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC driver_name
# MAGIC ,total_races
# MAGIC ,total_points
# MAGIC ,avg_points
# MAGIC ,RANK() OVER (ORDER BY avg_points DESC) as rank
# MAGIC FROM 
# MAGIC (
# MAGIC   SELECT 
# MAGIC   driver_name,
# MAGIC   count(1) as total_races,
# MAGIC   sum(calculated_points) as total_points,
# MAGIC   AVG(calculated_points) as avg_points
# MAGIC   FROM f1_presentation.calculated_race_results_vw
# MAGIC   WHERE race_year between 2010 and 2020
# MAGIC   GROUP BY driver_name
# MAGIC   ORDER BY avg_points DESC) temp
# MAGIC WHERE total_races >= 50;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
