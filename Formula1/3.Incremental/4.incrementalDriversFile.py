# Databricks notebook source
# MAGIC %run "../9.Includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

dbutils.widgets.text("p_file_name", "")
v_file_name = dbutils.widgets.get("p_file_name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE DATE

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR drivers.json FILE

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

name_schema = StructType(fields = 
 [
  StructField("forename", StringType(), True),
  StructField("surname", StringType(), True)
])

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

drivers_schema = StructType(fields = 
 [
  StructField("code", StringType(), True),
  StructField("dob", DateType(), True),
  StructField("driverId", IntegerType(), True),
  StructField("driverRef", StringType(), True),
  StructField("name", name_schema, True),
  StructField("nationality", StringType(), True),
  StructField("number", IntegerType(), True),
  StructField("url", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST constructors.json FILE

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f"{raw_path}/incremental/{v_file_date}/drivers.json")

display(drivers_df)
drivers_df.printSchema()
print(f"Number of Records Read {drivers_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### EXPLODE THE COLUMNS AS REQUIRED

# COMMAND ----------


from pyspark.sql.functions import col, current_timestamp, lit, concat

explode_drivers_df = drivers_df.select(
                                       col("code"), col("dob"), col("driverid").alias("driver_id"), 
                                       col("name.forename"), col("name.surname"), col("name"),
                                       col("driverRef").alias("driver_ref"),
                                       col("nationality"), col("number")
                                       ) \
.withColumn("fullname", concat(col("name.forename"), lit(" "), col("name.surname"))) \
.withColumn("file_name", lit(v_file_name)) \
.withColumn("file_date", lit(v_file_date)) \
.drop("name")

display(explode_drivers_df)

# COMMAND ----------

# MAGIC %run "../9.Includes/functions"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

drivers_final_df = ingest_dtm(explode_drivers_df) \
.select("driver_id", col("driver_ref"), 
                                             col("number"), "code", 
                                             col("fullname"),"dob",
                                             col("forename").alias("first_name"),
                                             col("surname").alias("last_name"),                                           
                                             col("nationality"),
                                             col("file_name"),
                                             col("file_date")
                                            ) 

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO DATALAKE AS PARQUET

# COMMAND ----------

# drivers_final_df.write.mode("overwrite").parquet(f"{incremental_path}/drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA WE WROTE TO DATALAKE BACK INTO A DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

# validate_drivers_df = spark.read \
# .parquet(f"{incremental_path}/drivers")

# display(validate_drivers_df)
# validate_drivers_df.printSchema()
# print(f"Number of Records Read {validate_drivers_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### REPLICATE THE DRIVERS DATA INSIDE PROCESSED DATABASE

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_incremental.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_incremental.drivers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS cnt from f1_incremental.drivers;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_incremental.drivers;
