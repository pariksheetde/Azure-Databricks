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
# MAGIC #### DEFINE SCHEMA FOR QUALIFYING DIRECTORY

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
.json(f"{raw_path}/qualifying")

display(qualifying_df)
qualifying_df.printSchema()
print(f"Number of Records Read {qualifying_df.count()}")
print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# MAGIC %run "../9.Includes/functions"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, concat

qualifying_renamed_df = ingest_dtm(qualifying_df) \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("file_name", lit(v_data_source))

display(qualifying_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### REPLICATE THE QUALIFYING DATA INSIDE PROCESSED DB

# COMMAND ----------

qualifying_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_etl.qualifying")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_etl.qualifying;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
