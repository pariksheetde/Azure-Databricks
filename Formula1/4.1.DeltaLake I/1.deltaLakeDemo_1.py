# Databricks notebook source
# MAGIC %md
# MAGIC #### 1.WRITE DATE TO DELTA LAKE (Managed Table)
# MAGIC #### 2.WRITE DATE TO DELTA LAKE (External Table)
# MAGIC #### 3.READ DATA FROM DELTA LAKE (Table)
# MAGIC #### 4.READ DATA FROM DELTA LAKE (File)

# COMMAND ----------

dbutils.widgets.text("p_data_source", "results")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import * 

results_df = spark.read \
.option("inferSchema", True) \
.json(f"/mnt/adobeadls/raw/incremental/{v_file_date}/results.json") \
.withColumn("load_ts", current_date()) \
.withColumn("file_name", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date)) \
.orderBy("constructorId")

display(results_df)
print(f"Number of records effected {results_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO DELTA LAKE (Managed Delta Table)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_delta.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS CNT
# MAGIC FROM f1_delta.results
# MAGIC GROUP BY file_date
# MAGIC ORDER BY 1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO DELTA LAKE (External Delta Table)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/adobeadls/deltalake/results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ DATA FROM DELTA LAKE (Table)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_delta.results_external;
# MAGIC CREATE TABLE IF NOT EXISTS f1_delta.results_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/adobeadls/deltalake/results"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS CNT
# MAGIC FROM f1_delta.results_external 
# MAGIC GROUP BY file_date
# MAGIC ORDER BY 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_delta.results_external 

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ DATA FROM DELTA LAKE (File)

# COMMAND ----------

validate_results_df = spark.read.format("delta").load("/mnt/adobeadls/deltalake/results") \
.sort("constructorId")

display(validate_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DML OPERATION ON DELTA LAKE (Update)

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_delta.results_external
# MAGIC SET points = 0
# MAGIC WHERE constructorId = 1 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_external WHERE constructorid = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### UPDATE DELTA LAKE

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

results_delta_table_upd = DeltaTable.forPath(spark, "/mnt/adobeadls/deltalake/results")
results_delta_table_upd.update("constructorId = 1", {"points" : "100"})

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_external
# MAGIC WHERE constructorid = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### DELETE RECORDS FROM DELTA

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

results_delta_table_del = DeltaTable.forPath(spark, "/mnt/adobeadls/deltalake/results")
results_delta_table_del.delete("constructorid = 1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_external
# MAGIC WHERE constructorid = 1;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE f1_delta.results;
