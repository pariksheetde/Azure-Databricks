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
# MAGIC #### DEFINE SCHEMA FOR CONSTRUCTORS.JSON FILE

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

constructor_schema = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST CONSTRUCTORS.JSON FILE

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

# MAGIC %run "../9.Includes/2.functions"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

rename_constructors_df = ingest_dtm(constructors_df).withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("constructorRef", "constructor_ref") \
.withColumn("file_name", lit(v_data_source)) \
.drop(col("url"))

display(rename_constructors_df)

# COMMAND ----------

# MAGIC %run "../0.setup/1.connection_parameter"

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO SNOWFLAKE DW
# MAGIC 1 DATABASE: RPT
# MAGIC 2.SCHEMA: HR
# MAGIC 3.TABLE: CONSTRUCTORS 

# COMMAND ----------

rename_constructors_df.write.format("snowflake").options(**connection_parameters) \
    .option("dbtable", "constructors").mode("overwrite").save()

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
