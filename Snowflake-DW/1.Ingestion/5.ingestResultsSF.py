# Databricks notebook source
# MAGIC %run "../9.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR RESULTS.JSON FILE

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
# MAGIC #### INGEST RESULTS.JSON FILE

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_path}/results.json")

display(results_df)
results_df.printSchema()
print(f"Number of Records Read {results_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# MAGIC %run "../9.Includes/2.functions"

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

# MAGIC %run "../0.setup/1.connection_parameter"

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO SNOWFLAKE DW
# MAGIC 1 DATABASE: RPT
# MAGIC 2.SCHEMA: HR
# MAGIC 3.TABLE: RESULTS 

# COMMAND ----------

results_renamed_df.write.format("snowflake").options(**connection_parameters) \
    .option("dbtable", "results").mode("overwrite").save()

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")