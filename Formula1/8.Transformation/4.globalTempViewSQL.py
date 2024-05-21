# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Dataframes using SQL
# MAGIC ##### 1. Create Global Temp viws on DataFrame
# MAGIC ##### 2. Access Views from SQL
# MAGIC ##### 3. Access Views from Python

# COMMAND ----------

# MAGIC %run "../9.Includes/1.config"

# COMMAND ----------

races_results_df = spark.read.parquet(f"{presentation_path}/race_results")
display(races_results_df)

# COMMAND ----------

races_results_df.createOrReplaceGlobalTempView("Race_Results_Global_Temp_VW")

# COMMAND ----------

# MAGIC %md
# MAGIC #### ACCESS VIEWS FROM SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC count(*) as no_of_races,
# MAGIC race_year
# MAGIC FROM global_temp.Race_Results_Global_Temp_VW
# MAGIC GROUP BY race_year
# MAGIC ORDER BY race_year desc;

# COMMAND ----------

# MAGIC %md
# MAGIC #### ACCESS VIEWS FROM PYTHON

# COMMAND ----------

sql_qry = spark.sql("""SELECT 
count(*) as no_of_races, 
race_year 
FROM global_temp.Race_Results_Global_Temp_VW 
GROUP BY race_year 
ORDER BY race_year desc""")
display(sql_qry)

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %md
# MAGIC #### TEMP VIEW CAN'NT BE EXECUTED FROM OTHER NOTEBOOK

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC count(*) as no_of_races,
# MAGIC race_year
# MAGIC FROM race_results_temp_vw
# MAGIC GROUP BY race_year
# MAGIC ORDER BY race_year desc;
