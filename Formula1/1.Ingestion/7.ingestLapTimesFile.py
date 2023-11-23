# Databricks notebook source
# MAGIC %run "../9.Includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR LAP_TIMES DIRECTORY

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
# MAGIC #### INGEST LAP_TIMES DIRECTORY

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
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# MAGIC %run "../9.Includes/functions"

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
# MAGIC #### READ THE DATA WE WROTE TO DATALAKE BACK INTO A DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

validate_lap_times_df = spark.read \
.parquet(f"{processed_path}/lap_times")

display(validate_lap_times_df)
validate_lap_times_df.printSchema()
print(f"Number of Records Read {validate_lap_times_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### REPLICATE THE PIT_STOPS DATA INSIDE PROCESSED DATABASE

# COMMAND ----------

# lap_times_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT COUNT(*) as cnt FROM f1_processed.lap_times;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
