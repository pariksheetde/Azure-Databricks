# Databricks notebook source
# MAGIC %md
# MAGIC #####Define schema for circuits.csv file

# COMMAND ----------

# MAGIC %run "../Includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Pass the parameter for the file name

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Pass the parameter for the file date

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

print(raw_path)

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType

circuits_schema = StructType(fields = 
 [
  StructField("circuitId", IntegerType(), True),
  StructField("circuitRef", StringType(), True),
  StructField("name", StringType(), True),
  StructField("location", StringType(), True),
  StructField("country", StringType(), True),
  StructField("lat", DoubleType(), True),
  StructField("lng", DoubleType(), True),
  StructField("alt", DoubleType(), True),
  StructField("url", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #####Ingest circuits.csv file

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_path}/incremental/{v_file_date}/circuits.csv")

display(circuits_df)
circuits_df.printSchema()
print(f"Number of Records Read {circuits_df.count()}")
print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Select required columns

# COMMAND ----------

from pyspark.sql.functions import col, lit
sel_circuits_df = circuits_df.select(
                                     col("circuitId").alias("circuit_id"), 
                                     col("circuitRef").alias("circuit_ref"),
                                     col("name"), "location", col("country"), 
                                     col("lat"), col("lng"), col("alt")
                                    )
display(sel_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Rename the columns as required

# COMMAND ----------

rename_circuits_df = sel_circuits_df.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("file_name", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

display(rename_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5. Add new columns

# COMMAND ----------

# MAGIC %run "../Includes/functions"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
# circuits_final_df = rename_circuits_df.withColumn("load_dtm", current_timestamp())
circuits_final_df = ingest_dtm(rename_circuits_df)

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write data to DataLake as parquet

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").parquet(f"{incremental_path}/circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read the data we wrote to DataLake back into a DataFrame to prove the write worked

# COMMAND ----------

# validate_circuits_df = spark.read \
# .parquet(f"{incremental_path}/circuits")

# display(validate_circuits_df)
# validate_circuits_df.printSchema()
# print(f"Number of Records Read {validate_circuits_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Replicate the circuits data inside incremental DB

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_incremental.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_incremental.circuits;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_incremental.circuits;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
