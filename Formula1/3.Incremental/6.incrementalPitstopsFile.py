# Databricks notebook source
# MAGIC %run "../9.Includes/1.config"

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
# MAGIC #### DEFINE SCHEMA FOR pit_stops.json FILE

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

pit_stops_schema = StructType(fields = 
 [
  StructField("driverId", IntegerType(), True),
  StructField("duration", StringType(), True),
  StructField("lap", IntegerType(), True),
  StructField("milliseconds", IntegerType(), True),
  StructField("raceId", IntegerType(), True),
  StructField("stop", StringType(), True),
  StructField("time", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST results.json FILE

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_path}/incremental/{v_file_date}/pit_stops.json")

display(pit_stops_df)
pit_stops_df.printSchema()
print(f"Number of Records Read {pit_stops_df.count()}")

print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# MAGIC %run "../9.Includes/2.functions"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, concat

pit_stops_renamed_df = ingest_dtm(pit_stops_df) \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("file_name", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date)) 

display(pit_stops_renamed_df)
print(f"Number of records {pit_stops_renamed_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO THE DATALAKE AS PARQUET FILE

# COMMAND ----------

# pit_stops_renamed_df.write.mode("append").partitionBy("race_id").parquet(f"{incremental_path}/pit_stops")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA TO DATALAKE BACK TO THE DATAFRAME TO CHECK THE WRITE WORKED

# COMMAND ----------

# validate_pit_stops_df = spark.read \
# .parquet(f"{incremental_path}/pit_stops")

# display(validate_pit_stops_df)
# validate_pit_stops_df.printSchema()
# print(f"Number of Records Read {validate_pit_stops_df.count()}")

# COMMAND ----------

sel_validate_pit_stops_df = pit_stops_renamed_df.select(col("driver_id"), col("duration"), col("lap"), col("milliseconds"), col("stop"),
                                                        col("time"), col("load_ts"), col("file_name"), col("file_date"), col("race_id"))

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC #### INCREMENTAL LOAD USING insertInto()

# COMMAND ----------

if (spark._jsparkSession.catalog().tableExists("f1_incremental.pit_stops")):
  sel_validate_pit_stops_df.write.mode("overwrite").insertInto("f1_incremental.pit_stops")
else:
  sel_validate_pit_stops_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_incremental.pit_stops")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC race_id
# MAGIC ,COUNT(race_id) as cnt
# MAGIC FROM f1_incremental.pit_stops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC COUNT(*) as cnt,
# MAGIC file_date 
# MAGIC FROM f1_incremental.pit_stops
# MAGIC GROUP BY file_date
# MAGIC ORDER BY file_date ASC;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE f1_incremental.pit_stops;
