# Databricks notebook source
# MAGIC %run "../Includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Pass the parameter for the file name

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Define schema for lap_times directory

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType

laps_schema = StructType(fields = 
 [
  StructField("raceId", IntegerType(), True),
  StructField("driverId", IntegerType(), True),
  StructField("lap", IntegerType(), True),
  StructField("position", IntegerType(), True),
  StructField("time", StringType(), True),
  StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #####Ingest lap_times directory

# COMMAND ----------

lap_times_df = spark.read \
.schema(laps_schema) \
.csv(f"{raw_path}/lap_times/lap_times_*")

display(lap_times_df)
lap_times_df.printSchema()
print(f"Number of Records Read {lap_times_df.count()}")

print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Rename the columns as required

# COMMAND ----------

# MAGIC %run "../Includes/functions"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, concat, input_file_name

lap_times_renamed_df = ingest_dtm(lap_times_df) \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("source_file_name", input_file_name()) \
.withColumn("file_name", lit(v_data_source))

display(lap_times_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write data to DataLake as parquet

# COMMAND ----------

lap_times_renamed_df.write.mode("overwrite").parquet(f"{processed_path}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read the data we wrote to DataLake back into a DataFrame to prove the write worked

# COMMAND ----------

validate_lap_times_df = spark.read \
.parquet(f"{processed_path}/lap_times")

display(validate_lap_times_df)
validate_lap_times_df.printSchema()
print(f"Number of Records Read {validate_lap_times_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Replicate the lap_times data inside Processed DB

# COMMAND ----------

# lap_times_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT COUNT(*) as cnt FROM f1_processed.lap_times;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
