# Databricks notebook source
# MAGIC %md
# MAGIC ##### MOST DOMINANT TEAMS IN FORMULA 1

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.dominant_teams;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.dominant_teams
# MAGIC (
# MAGIC team_name STRING,
# MAGIC total_races INT,
# MAGIC total_points INT,
# MAGIC avg_points INT,
# MAGIC rank INT
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_presentation.dominant_teams
# MAGIC SELECT 
# MAGIC team_name
# MAGIC ,total_races
# MAGIC ,total_points
# MAGIC ,avg_points
# MAGIC ,RANK() OVER (ORDER BY avg_points DESC) as rank
# MAGIC FROM 
# MAGIC (
# MAGIC   SELECT 
# MAGIC   team_name,
# MAGIC   count(1) as total_races,
# MAGIC   sum(calculated_points) as total_points,
# MAGIC   AVG(calculated_points) as avg_points
# MAGIC   FROM f1_presentation.calculated_race_results_vw
# MAGIC   GROUP BY team_name
# MAGIC   ORDER BY avg_points DESC) temp
# MAGIC WHERE total_races >= 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.dominant_teams;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### MOST DOMINANT TEAMS IN LAST DECADE IN FORMULA 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC team_name
# MAGIC ,total_races
# MAGIC ,total_points
# MAGIC ,avg_points
# MAGIC ,RANK() OVER (ORDER BY avg_points DESC) as rank
# MAGIC FROM
# MAGIC (
# MAGIC   SELECT 
# MAGIC   team_name,
# MAGIC   count(1) as total_races,
# MAGIC   sum(calculated_points) as total_points,
# MAGIC   AVG(calculated_points) as avg_points
# MAGIC   FROM f1_presentation.calculated_race_results_vw
# MAGIC   WHERE race_year between 2010 and 2020
# MAGIC   GROUP BY team_name
# MAGIC   ORDER BY avg_points DESC) temp
# MAGIC WHERE total_races >= 100;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
