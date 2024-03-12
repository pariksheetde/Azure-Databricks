# Databricks notebook source
# MAGIC %md
# MAGIC #### LOAD ORDERS DATA FROM MULTIPLEX BRONZE TABLE INTO KAFKA.CUSTOMERS_SILVER TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS kafka.customers_silver
# MAGIC (
# MAGIC   customer_id STRING, 
# MAGIC   email STRING, 
# MAGIC   first_name STRING, 
# MAGIC   last_name STRING, 
# MAGIC   gender STRING, 
# MAGIC   street STRING, 
# MAGIC   city STRING, 
# MAGIC   country STRING, 
# MAGIC   row_time TIMESTAMP
# MAGIC )
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# %sql
# ALTER TABLE kafka.customer_silvers
# SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

country_lookup_df = spark.read.json("/mnt/adobeadls/dwanalytics/orders/kafka/country_lookup")
display(country_lookup_df)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

def customer_upsert(microBatchDF, batch):
    window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())
    
    microBatchDF.filter(F.col("row_status").isin(['insert', 'update'])) \
                 .withColumn("rank", F.rank().over(window)) \
                 .filter("rank == 1") \
                 .createOrReplaceTempView("ranked_updates")
                

    sql_qry = """
            MERGE INTO kafka.customers_silver cust
            USING ranked_updates upd
            ON cust.customer_id = upd.customer_id
                WHEN MATCHED AND cust.row_time < upd.row_time 
                    THEN UPDATE SET *
                WHEN NOT MATCHED 
                    THEN INSERT *
        """
    microBatchDF.sparkSession.sql(sql_qry)


# COMMAND ----------

from pyspark.sql import functions as F

schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING,  country_code STRING, row_status STRING, row_time TIMESTAMP"

query = (spark.readStream
         .table("kafka.bronze")
         .filter("topic = 'customers'")
         .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
         .select("v.*")
         .join(F.broadcast(country_lookup_df),F.col("country_code") == F.col("code"), "inner")
         .writeStream
         .foreachBatch(customer_upsert)
         .option("checkpointLocation", "/mnt/adobeadls/dwanalytics/orders/kafka/checkpoint/customers/silver")
         .trigger(availableNow=True)
         .start()
      )
query.awaitTermination()

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
