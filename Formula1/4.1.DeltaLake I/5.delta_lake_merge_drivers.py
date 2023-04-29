# Databricks notebook source
# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### READ 1st day's driver data

# COMMAND ----------

from pyspark.sql.functions import col

driver_day1_df = spark.read.json(f"{raw_path}/incremental/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", col("name.forename").alias("firstname"), col("name.surname").alias("lastname"))

display(driver_day1_df)
print(f"Number of Records {driver_day1_df.count()}")

# COMMAND ----------

driver_day1_df.createOrReplaceTempView("driver_day1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM driver_day1;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### READ 2nd day's driver data

# COMMAND ----------

from pyspark.sql.functions import col, upper

driver_day2_df = spark.read.json(f"{raw_path}/incremental/2021-04-18/drivers.json") \
.filter("driverId BETWEEN 6 AND 15") \
.select("driverId", "dob", upper(col("name.forename")).alias("firstname"), upper(col("name.surname")).alias("lastname"))

display(driver_day2_df)
print(f"Number of Records {driver_day2_df.count()}")

# COMMAND ----------

driver_day2_df.createOrReplaceTempView("driver_day2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM driver_day2;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### READ 3rd day's driver data

# COMMAND ----------

from pyspark.sql.functions import col, upper

driver_day3_df = spark.read.json(f"{raw_path}/incremental/2021-04-18/drivers.json") \
.filter("driverId BETWEEN 6 AND 15 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", upper(col("name.forename")).alias("firstname"), upper(col("name.surname")).alias("lastname"))

display(driver_day3_df)
print(f"Number of Records {driver_day3_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_delta.driver_merge
# MAGIC (
# MAGIC driver_id INT,
# MAGIC dob DATE,
# MAGIC firstname STRING,
# MAGIC lastname STRING,
# MAGIC created_dt DATE,
# MAGIC updated_dt DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.driver_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### DAY 1 MERGE INTO TARGET TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_delta.driver_merge tgt
# MAGIC USING driver_day1 src
# MAGIC ON (src.driverId = tgt.driver_id)
# MAGIC WHEN MATCHED 
# MAGIC   THEN UPDATE SET tgt.dob = src.dob,
# MAGIC              tgt.firstname = src.firstname,
# MAGIC              tgt.lastname = src.lastname,
# MAGIC              tgt.updated_dt = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (tgt.driver_id, firstname, lastname, tgt.created_dt) VALUES (src.driverid, src.firstname, src.lastname, CURRENT_TIMESTAMP)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.driver_merge tgt;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### DAY 2 MERGE INTO TARGET TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_delta.driver_merge tgt
# MAGIC USING driver_day2 src
# MAGIC ON (src.driverId = tgt.driver_id)
# MAGIC WHEN MATCHED 
# MAGIC   THEN UPDATE SET tgt.dob = src.dob,
# MAGIC              tgt.firstname = src.firstname,
# MAGIC              tgt.lastname = src.lastname,
# MAGIC              tgt.updated_dt = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (tgt.driver_id, firstname, lastname, tgt.created_dt) VALUES (src.driverid, src.firstname, src.lastname, CURRENT_TIMESTAMP)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.driver_merge tgt;
