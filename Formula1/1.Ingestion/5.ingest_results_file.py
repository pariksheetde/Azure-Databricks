# Databricks notebook source
# MAGIC %run "../9.Includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Pass the parameter for the file name

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Define schema for results.json file

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

results_schema = StructType(fields = 
 [
  StructField("constructorId", IntegerType(), True),
  StructField("driverId", IntegerType(), True),
  StructField("fastestLap", IntegerType(), True),
  StructField("fastestLapSpeed", FloatType(), True),
  StructField("fastestLapTime", StringType(), True),
  StructField("grid", IntegerType(), True),
  StructField("laps", IntegerType(), True),
  StructField("milliseconds", IntegerType(), True),
  StructField("number", IntegerType(), True),
  StructField("points", FloatType(), True),
  StructField("position", IntegerType(), True),
  StructField("positionOrder", IntegerType(), True),
  StructField("positionText", StringType(), True),
  StructField("raceId", IntegerType(), True),
  StructField("rank", IntegerType(), True),
  StructField("resultId", IntegerType(), True),
  StructField("statusId", StringType(), True),
  StructField("time", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #####Ingest results.json file

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_path}/results.json")

display(results_df)
results_df.printSchema()
print(f"Number of Records Read {results_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Rename the columns as required

# COMMAND ----------

# MAGIC %run "../Includes/functions"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, concat

results_renamed_df = ingest_dtm(results_df) \
.withColumnRenamed("resultId", "result_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("positionText", "position_text") \
.withColumnRenamed("positionOrder", "position_order") \
.withColumnRenamed("fastestLap", "fastest_lap") \
.withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
.withColumnRenamed("fastestLapTime", "fastest_lap_time") \
.withColumn("file_name", lit(v_data_source)) \
.drop("statusId")

display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write data to DataLake as parquet

# COMMAND ----------

results_renamed_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_path}/results")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read the data we wrote to DataLake back into a DataFrame to prove the write worked

# COMMAND ----------

validate_drivers_df = spark.read \
.parquet(f"{processed_path}/results")

display(validate_drivers_df)
validate_drivers_df.printSchema()
print(f"Number of Records Read {validate_drivers_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Replicate the results data inside Processed DB

# COMMAND ----------

# results_renamed_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT COUNT(*) as cnt FROM f1_processed.results;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
