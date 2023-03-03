# Databricks notebook source
# MAGIC %run "../Includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read from Races DataSet

# COMMAND ----------

races_df = spark.read \
.parquet(f"{processed_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date")

display(races_df)
print(f"Number of Records Read {races_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read from Circuits DataSet

# COMMAND ----------

circuit_df = spark.read \
.parquet(f"{processed_path}/circuits") \
.withColumnRenamed("location", "circuit_location")

display(circuit_df)
print(f"Number of Records Read {circuit_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read from Drivers DataSet

# COMMAND ----------

drivers_df = spark.read \
.parquet(f"{processed_path}/drivers") \
.withColumnRenamed("fullname", "driver_name") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("nationality", "driver_nationality")

display(drivers_df)
print(f"Number of Records Read {drivers_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read from Constructors DataSet

# COMMAND ----------

constructors_df = spark.read \
.parquet(f"{processed_path}/constructors") \
.withColumnRenamed("constructor_id", "cons_id") \
.withColumnRenamed("name", "team")

display(constructors_df)
print(f"Number of Records Read {constructors_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read from Results DataSet

# COMMAND ----------

results_df = spark.read \
.parquet(f"{processed_path}/results") \
.withColumnRenamed("time", "race_time")

display(results_df)
print(f"Number of Records Read {results_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join Between Races & Circuits

# COMMAND ----------

join_races_circuits_df = races_df.join(circuit_df, races_df.circuit_id == circuit_df.circuit_id, "inner") \
.select("race_id", "race_year", "race_name", "race_date", "circuit_location")
display(join_races_circuits_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

join_race_results_df = results_df.join(join_races_circuits_df, results_df.race_id == join_races_circuits_df.race_id, "inner") \
                                    .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
                                    .join(constructors_df, results_df.constructor_id == constructors_df.cons_id, "inner") \
.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", 
        "team", "grid", "fastest_lap", "race_time", "points", "position") \
.orderBy(col("points").desc()) \
.withColumn("created_dt", current_timestamp())

display(join_race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Save the data in the Presentation DataLake

# COMMAND ----------

join_race_results_df.write.mode("overwrite").parquet(f"{presentation_path}/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Save the data in the Presentation DB

# COMMAND ----------

join_race_results_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

print(f"Number of Records {join_race_results_df.count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.race_results;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
