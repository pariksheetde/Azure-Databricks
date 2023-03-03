# Databricks notebook source
# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR CIRCUITS.CSV FILE
# MAGIC 
# MAGIC ####----------------------------------------------------------------------------------
# MAGIC 1. Pass the parameter for the file name
# MAGIC 2. Ingest circuits.csv file
# MAGIC 3. Remove non numeric data from percentage
# MAGIC 4. Pivot the data by age group
# MAGIC 5. Join to dim_country to get the country, 3 digit country code and the total population.
# MAGIC 
# MAGIC ####-----------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define the paths for different environments

# COMMAND ----------

# MAGIC %run "../Includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

print(v_data_source)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR CIRCUITS.CSV

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
# MAGIC #### STEP 2. INGEST CIRCUITS.CSV FILE

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_path}/circuits.csv")

display(circuits_df)
circuits_df.printSchema()
print(f"Number of Records Read {circuits_df.count()}")
print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SELECT REQUIRED COLUMNS THAT NEEDS TO BE PROCESSED

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
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

rename_circuits_df = sel_circuits_df.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("file_name", lit(v_data_source))

display(rename_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ADD NEW COLUMNS

# COMMAND ----------

# MAGIC %run "../Includes/functions"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
# circuits_final_df = rename_circuits_df.withColumn("load_dtm", current_timestamp())
circuits_final_df = ingest_dtm(rename_circuits_df)

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO DATALAKE AS PARQUET

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_path}/circuits")
print(processed_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA WE WROTE TO DATALAKE BACK INTO A DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

validate_circuits_df = spark.read \
.parquet(f"{processed_path}/circuits")

display(validate_circuits_df)
validate_circuits_df.printSchema()
print(f"Number of Records Read {validate_circuits_df.count()}")
print(processed_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM parquet.`f"{processed_path}/circuits"`;

# COMMAND ----------

# MAGIC %md
# MAGIC #### REPLICATE THE CIRCUITS DATA INSIDE PROCESSED DATABASE

# COMMAND ----------

# validate_circuits_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT COUNT(*) as cnt FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
