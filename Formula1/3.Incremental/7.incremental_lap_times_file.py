# Databricks notebook source
# MAGIC %run "../Includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
print(v_data_source)

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE DATE

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")
print(v_file_date)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR lap_times DIRECTORY

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
# MAGIC #### INGEST lap_times DIRECTORY

# COMMAND ----------

lap_times_df = spark.read \
.schema(laps_schema) \
.csv(f"{raw_path}/incremental/{v_file_date}/lap_times/lap_times_*")

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

from pyspark.sql.functions import col, current_timestamp, lit, concat

lap_times_renamed_df = ingest_dtm(lap_times_df) \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("file_name", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

display(lap_times_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE THE DATA TO THE DATALAKE AS PARQUET FILE 

# COMMAND ----------

# lap_times_renamed_df.write.mode("append").partitionBy("race_id").parquet(f"{incremental_path}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA WE WROTE TO DATALAKE BACK INTO A DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

# validate_lap_times_df = spark.read \
# .parquet(f"{incremental_path}/lap_times")

# display(validate_lap_times_df)
# validate_lap_times_df.printSchema()
# print(f"Number of Records Read {validate_lap_times_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### REPLICATE THE LAP_TIMES DATA INSIDE INCREMENTAL DB

# COMMAND ----------

validate_final_lap_times_df = lap_times_renamed_df.select(col("driver_id"), col("lap"), col("position"), col("time"), col("milliseconds"),
                                                          col("load_ts"), col("file_name"), col("file_date"), col("race_id"))

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

if (spark._jsparkSession.catalog().tableExists("f1_incremental.lap_times")):
  validate_final_lap_times_df.write.mode("overwrite").insertInto("f1_incremental.lap_times")
else:
  validate_final_lap_times_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_incremental.lap_times")

# COMMAND ----------

# %sql
# SELECT
# race_id
# ,COUNT(*) as cnt FROM 
# f1_incremental.lap_times
# GROUP BY race_id
# ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC COUNT(*) as cnt,
# MAGIC file_date 
# MAGIC FROM 
# MAGIC f1_incremental.lap_times
# MAGIC GROUP BY 2
# MAGIC ORDER BY file_date ASC;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE f1_incremental.lap_times;
