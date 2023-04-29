# Databricks notebook source
# MAGIC %run "../Includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Pass the parameter for the file name

# COMMAND ----------

dbutils.widgets.text("p_data_source", "results")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Pass the parameter for the file date

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

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
# MAGIC #####Read results.json file

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_path}/incremental/{v_file_date}/results.json")

display(results_df)
results_df.printSchema()
print(f"Number of Records Read {results_df.count()}")

print(raw_path)

# COMMAND ----------

# MAGIC %run "../Includes/functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Rename the columns as required

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
.withColumn("file_date", lit(v_file_date)) \
.drop("statusId")

display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Select the required columns

# COMMAND ----------

results_final_df = results_renamed_df.select(col("constructor_id"), col("driver_id"), col("fastest_lap"), col("fastest_lap_speed"), col("fastest_lap_time"),
                                            col("grid"), col("laps"), col("milliseconds"), col("number"), col("points"), col("position"),
                                            col("position_order"), col("position_text"), col("rank"), col("result_id"), col("time"), col("load_ts"),
                                            col("file_name"), col("file_date"), col("race_id"))
display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### De-Duped Data ELimination

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write data to DataLake as parquet

# COMMAND ----------

from delta.tables import DeltaTable
spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

if (spark._jsparkSession.catalog().tableExists("f1_delta.results")):
  deltaTable = DeltaTable.forPath(spark, "/mnt/adobeadls/deltalake/results")
  deltaTable.alias("tgt").merge(
    results_deduped_df.alias("src"),
    "tgt.result_id = src.result_id AND tgt.race_id = src.race_id") \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()
else:
  results_deduped_df.write.mode("overwrite").partitionBy("race_id").format("delta").saveAsTable("f1_delta.results")

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#   if (spark._jsparkSession.catalog().tableExists("f1_delta.results")):
#     spark.sql(f"ALTER TABLE f1_incremental.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id").parquet(f"{incremental_path}/results")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read the data we wrote to DataLake back into a DataFrame to prove the write worked

# COMMAND ----------

# validate_drivers_df = spark.read \
# .parquet(f"{incremental_path}/results")

# display(validate_drivers_df)
# validate_drivers_df.printSchema()
# print(f"Number of Records Read {validate_drivers_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Incremental Load using append()

# COMMAND ----------

# %sql
# SELECT COUNT(*) AS CNT FROM f1_delta.results;

# COMMAND ----------

# %sql
# SELECT 
#   race_id, count(race_id) as cnt 
#   FROM f1_delta.results
#   GROUP BY race_id
#   ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC file_date, 
# MAGIC count(*) as CNT
# MAGIC FROM f1_delta.results
# MAGIC GROUP BY 1;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table f1_delta.results;
