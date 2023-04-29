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
# MAGIC ###Step 1. Define schema for constructors.json file

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

constructor_schema = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2. Ingest constructors.json file

# COMMAND ----------

constructors_df = spark.read \
.schema(constructor_schema) \
.json(f"{raw_path}/incremental/{v_file_date}/constructors.json")

display(constructors_df)
constructors_df.printSchema()
print(f"Number of Records Read {constructors_df.count()}")
print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3. Rename the columns as required

# COMMAND ----------

# MAGIC %run "../Includes/functions"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

rename_constructors_df = ingest_dtm(constructors_df).withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("constructorRef", "constructor_ref") \
.withColumn("file_name", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date)) \
.drop(col("url"))

display(rename_constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4. Write data to datalake as parquet

# COMMAND ----------

# rename_constructors_df.write.mode("overwrite").parquet(f"{incremental_path}/constructors")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6. Read the data we wrote to DataLake back into a DataFrame to prove the write worked

# COMMAND ----------

# validate_constructors_df = spark.read \
# .parquet(f"{incremental_path}/constructors")

# display(validate_constructors_df)
# validate_constructors_df.printSchema()
# print(f"Number of Records Read {validate_constructors_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### STEP 7. Replicate the circuits data inside processed database

# COMMAND ----------

rename_constructors_df.write.mode("overwrite").format("parquet").saveAsTable("f1_incremental.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_incremental.constructors;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_incremental.constructors;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_incremental.constructors;
