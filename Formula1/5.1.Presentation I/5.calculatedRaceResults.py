# Databricks notebook source
# MAGIC %sql
# MAGIC use f1_etl;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW f1_presentation.calculated_race_results_vw
# MAGIC AS
# MAGIC SELECT
# MAGIC races.race_year,
# MAGIC constructors.name as team_name,
# MAGIC drivers.fullname as driver_name,
# MAGIC results.position,
# MAGIC results.points,
# MAGIC (11 - results.position) as calculated_points
# MAGIC   FROM f1_etl.results, f1_etl.drivers, f1_etl.constructors, f1_etl.races
# MAGIC   WHERE results.driver_id = drivers.driver_id
# MAGIC   AND results.constructor_id = constructors.constructor_id
# MAGIC   AND results.race_id = races.race_id
# MAGIC AND results.position <= 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.calculated_race_results_vw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS CNT FROM f1_presentation.calculated_race_results_vw;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
