# Databricks notebook source
# MAGIC %run "../9.Includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

dbutils.widgets.text("p_data_source", "qualifying")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE DATE

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR QUALIFYING FILE

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

qualifying_schema = StructType(fields = 
 [
  StructField("constructorId", IntegerType(), True),
  StructField("driverId", IntegerType(), True),
  StructField("number", IntegerType(), True),
  StructField("position", IntegerType(), True),
  StructField("q1", StringType(), True),
  StructField("q2", StringType(), True),
  StructField("q3", StringType(), True),
  StructField("qualifyId", IntegerType(), True),
  StructField("raceId", IntegerType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST QUALIFYING DIRECTORY

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_path}/incremental/{v_file_date}/qualifying")

display(qualifying_df)
qualifying_df.printSchema()
print(f"Number of Records Read {qualifying_df.count()}")
print(raw_path)

# COMMAND ----------

# MAGIC %run "../9.Includes/functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### REMAME THE COLUMNS AS REQUIRED

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, concat

qualifying_renamed_df = ingest_dtm(qualifying_df) \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("file_name", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

display(qualifying_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO DATALAKE AS PARQUET FILE

# COMMAND ----------

# qualifying_renamed_df.write.mode("append").partitionBy("race_id").parquet(f"{incremental_path}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA WE WROTE TO DATALAKE BACK INTO A DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

# validate_qualifying_df = spark.read \
# .parquet(f"{incremental_path}/qualifying")

# display(validate_qualifying_df)
# validate_qualifying_df.printSchema()
# print(f"Number of Records Read {validate_qualifying_df.count()}")

# COMMAND ----------

qualifying_final_df = qualifying_renamed_df.select(col("constructor_id"), col("driver_id"), col("number"), col("position"), col("q1"),
                                            col("q2"), col("q3"), col("qualify_id"), col("load_ts"), col("file_name"), col("file_date"),
                                            col("race_id"))
display(qualifying_final_df)

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

if (spark._jsparkSession.catalog().tableExists("f1_incremental.qualifying")):
  qualifying_final_df.write.mode("overwrite").insertInto("f1_incremental.qualifying")
else:
  qualifying_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_incremental.qualifying")

# COMMAND ----------

# %sql
# SELECT 
# race_id
# ,COUNT(*) as cnt 
# FROM f1_incremental.qualifying
# GROUP BY race_id
# ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC COUNT(*) as cnt,
# MAGIC file_date
# MAGIC FROM f1_incremental.qualifying
# MAGIC GROUP BY 2;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE f1_incremental.qualifying;
