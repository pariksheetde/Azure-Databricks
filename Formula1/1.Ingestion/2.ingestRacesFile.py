# Databricks notebook source
# MAGIC %run "../9.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR RACES.CSV FILE

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
# MAGIC #### INGEST RACES.CSV FILE

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_path}/races.csv")

display(races_df)
races_df.printSchema()
print(f"Number of Records Read {races_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### SELECT REQUIRED COLUMNS

# COMMAND ----------

from pyspark.sql.functions import col, lit
sel_races_df = races_df.select(
                               col("race_id"), col("year"), col("round"), "circuitid", col("name"), 
                               col("date"), col("time"), col("url")
                                    )
display(sel_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME / DROP THE COLUMNS AS REQUIRED

# COMMAND ----------

rename_races_df = sel_races_df.withColumnRenamed("circuitid", "circuit_id") \
.withColumnRenamed("year", "race_year") \
.drop(col("url")) \
.withColumn("file_name", lit(v_data_source))

display(rename_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ADD NEW COLUMNS

# COMMAND ----------

# MAGIC %run "../9.Includes/2.functions"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, col, to_timestamp, concat

races_with_timestamp_df = ingest_dtm(rename_races_df).withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(' '), col("time")), 'yyyy-MM-dd HH:mm:ss')) \
.drop("date", "time")

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO DATALAKE AS PARQUET

# COMMAND ----------

races_with_timestamp_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_path}/races")
print(processed_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA WE WROTE TO DATALAKE BACK INTO A DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

validate_races_df = spark.read \
.parquet(f"{processed_path}/races")

display(validate_races_df)
validate_races_df.printSchema()
print(f"Number of Records Read {validate_races_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### REPLICATE THE RACES DATA INSIDE PROCESSED DATABASE

# COMMAND ----------

# races_with_timestamp_df.write.mode("overwrite").partitionBy("race_year").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT COUNT(*) as cnt from f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
