# Databricks notebook source
# MAGIC %run "../9.Includes/config"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_path}/race_results") \
.filter("race_year in (2019,2020)")
display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### COUNT OF RACES

# COMMAND ----------

from pyspark.sql.functions import count
count_race_name_df = race_results_df.select(count("race_name").alias("count_of_races"))
display(count_race_name_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### NUMBER OF RACES, POINTS BY Lewis Hamilton

# COMMAND ----------

from pyspark.sql.functions import *
agg_Lewis_Hamilton_df = race_results_df.filter("driver_name = 'Lewis Hamilton'") \
.groupBy("race_year", "driver_name") \
.agg(sum("points").alias("sum_points"), count("race_name").alias("count_races")) \
.orderBy(col("sum_points").desc())

display(agg_Lewis_Hamilton_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### NUMBER OF RACES, POINTS BY ALL DRIVERS

# COMMAND ----------

from pyspark.sql.functions import *

agg_all_drivers_df = race_results_df.groupBy("race_year", "driver_name") \
.agg(sum("points").alias("sum_points"), count("race_name").alias("count_races")) \
.orderBy(col("race_year").asc(),col("sum_points").desc())

display(agg_all_drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WINDOW FUNCTIONS

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

windowSpec = Window \
    .partitionBy("race_year") \
    .orderBy(col("sum_points").desc())

win_all_drivers_df = agg_all_drivers_df.select("race_year", "sum_points", "driver_name") \
.withColumn("rank", rank().over(windowSpec))

display(win_all_drivers_df)
