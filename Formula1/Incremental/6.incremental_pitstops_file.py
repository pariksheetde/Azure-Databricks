# Databricks notebook source
# MAGIC %run "../Includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Pass the parameter for the file name

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
print(v_data_source)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Pass the parameter for the file date

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")
print(v_file_date)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Define schema for pit_stops.json file

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
# MAGIC #####Ingest results.json file

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
# MAGIC #####Rename the columns as required

# COMMAND ----------

# MAGIC %run "../Includes/functions"

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
# MAGIC #####Write data to DataLake as parquet

# COMMAND ----------

# pit_stops_renamed_df.write.mode("append").partitionBy("race_id").parquet(f"{incremental_path}/pit_stops")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read the data we wrote to DataLake back into a DataFrame to prove the write worked

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
# MAGIC #####Incremental Load using insertInto()

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
# MAGIC GROUP BY 2;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE f1_incremental.pit_stops;
