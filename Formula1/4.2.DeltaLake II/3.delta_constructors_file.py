# Databricks notebook source
# MAGIC %run "../9.Includes/config" 

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR constructors.json FILE

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

constructor_schema = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST constructors.json FILE

# COMMAND ----------

constructors_df = spark.read \
.schema(constructor_schema) \
.json(f"{raw_path}/constructors.json")

display(constructors_df)
constructors_df.printSchema()
print(f"Number of Records Read {constructors_df.count()}")
print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# MAGIC %run "../9.Includes/functions"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

rename_constructors_df = ingest_dtm(constructors_df).withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("constructorRef", "constructor_ref") \
.withColumn("file_name", lit(v_data_source)) \
.drop(col("url"))

display(rename_constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### LOAD CONSTRUCTORS DATA INSIDE DELTA DB

# COMMAND ----------

rename_constructors_df.write.mode("overwrite").format("delta").saveAsTable("f1_delta.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt from f1_delta.constructors;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
