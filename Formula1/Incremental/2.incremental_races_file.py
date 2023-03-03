# Databricks notebook source
# MAGIC %run "../Includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pass the parameter for the file name

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pass the parameter for the file date

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1. Define schema for races.csv file

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

races_schema = StructType(fields = 
 [
  StructField("race_id", IntegerType(), True),
  StructField("year", IntegerType(), True),
  StructField("round", IntegerType(), True),
  StructField("circuitid", IntegerType(), True),
  StructField("name", StringType(), True),
  StructField("date", DateType(), True),
  StructField("time", StringType(), True),
  StructField("url", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2. Ingest races.csv file

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_path}/incremental/{v_file_date}/races.csv")

display(races_df)
races_df.printSchema()
print(f"Number of Records Read {races_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3. Select required columns

# COMMAND ----------

from pyspark.sql.functions import col, lit
sel_races_df = races_df.select(
                                     col("race_id"), col("year"),col("round"), "circuitid", col("name"), 
                                     col("date"), col("time"), col("url")
                                    )
display(sel_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4. Rename the columns as required

# COMMAND ----------

rename_races_df = sel_races_df.withColumnRenamed("circuitid", "circuit_id") \
.withColumnRenamed("year", "race_year") \
.drop(col("url")) \
.withColumn("file_name", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

display(rename_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5. Add new columns

# COMMAND ----------

# MAGIC %run "../Includes/functions"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, col, to_timestamp, concat

races_with_timestamp_df = ingest_dtm(rename_races_df).withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(' '), col("time")), 'yyyy-MM-dd HH:mm:ss')) \
.drop("date", "time")

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6. Write data to DataLake as parquet

# COMMAND ----------

# races_with_timestamp_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{incremental_path}/races")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7. Read the data we wrote to DataLake back into a DataFrame to prove the write worked

# COMMAND ----------

# validate_races_df = spark.read \
# .parquet(f"{incremental_path}/races")

# display(validate_races_df)
# validate_races_df.printSchema()
# print(f"Number of Records Read {validate_races_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### STEP 8. Replicate the races data inside processed database

# COMMAND ----------

races_with_timestamp_df.write.mode("overwrite").partitionBy("race_year").format("parquet").saveAsTable("f1_incremental.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_incremental.races;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_incremental.races;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
