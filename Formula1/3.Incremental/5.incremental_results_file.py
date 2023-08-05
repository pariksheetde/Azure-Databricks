# Databricks notebook source
# MAGIC %run "../9.Includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

dbutils.widgets.text("p_data_source", "results")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE DATE

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE THE SCHEMA FOR results.json FILE

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
# MAGIC #### READ results.json FILE

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_path}/incremental/{v_file_date}/results.json")

display(results_df)
results_df.printSchema()
print(f"Number of Records Read {results_df.count()}")

print(raw_path)

# COMMAND ----------

# MAGIC %run "../9.Includes/functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

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
# MAGIC #### SELECT THE REQUIRED COLUMNS

# COMMAND ----------

results_final_df = results_renamed_df.select(col("constructor_id"), col("driver_id"), col("fastest_lap"), col("fastest_lap_speed"), col("fastest_lap_time"),
                                            col("grid"), col("laps"), col("milliseconds"), col("number"), col("points"), col("position"),
                                            col("position_order"), col("position_text"), col("rank"), col("result_id"), col("time"), col("load_ts"),
                                            col("file_name"), col("file_date"), col("race_id"))
display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE THE DATA TO THE DATALAKE AS PARQUET FILE

# COMMAND ----------

for race_id_list in results_final_df.select("race_id").distinct().collect():
  if (spark._jsparkSession.catalog().tableExists("f1_incremental.results")):
    spark.sql(f"ALTER TABLE f1_incremental.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id").parquet(f"{incremental_path}/results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA WE WROTE TO THE DATALAKE BACK TO THE DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

# validate_drivers_df = spark.read \
# .parquet(f"{incremental_path}/results")

# display(validate_drivers_df)
# validate_drivers_df.printSchema()
# print(f"Number of Records Read {validate_drivers_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### INCREMENTAL LOAD USING append()

# COMMAND ----------

results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_incremental.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   race_id, count(race_id) as cnt
# MAGIC   FROM f1_incremental.results
# MAGIC   GROUP BY race_id
# MAGIC   ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT COUNT(*) AS CNT FROM f1_incremental.results;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table f1_incremental.results;
