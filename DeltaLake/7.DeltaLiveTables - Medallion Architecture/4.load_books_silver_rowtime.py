# Databricks notebook source
# MAGIC %md
# MAGIC #### LOAD ORDERS DATA FROM MULTIPLEX BRONZE TABLE INTO KAFKA.BOOKS_SILVER TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS kafka.books_silver_rowtime
# MAGIC (
# MAGIC   book_id STRING,
# MAGIC   title STRING,
# MAGIC   author STRING,
# MAGIC   price DOUBLE,
# MAGIC   current INT,
# MAGIC   effective_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP,
# MAGIC   row_time TIMESTAMP
# MAGIC )
# MAGIC TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, 
# MAGIC                delta.autoOptimize.autoCompact = true)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

def books_upsert(microBatchDF, batch):
    window = Window.partitionBy("book_id").orderBy(F.col("row_time").desc())
    
    microBatchDF.withColumn("rank", F.rank().over(window)) \
                 .filter("rank == 1") \
                 .createOrReplaceTempView("ranked_updates_books")
                

    sql_qry = """
            MERGE INTO kafka.books_silver_rowtime
    USING(
        SELECT ranked_updates_books.book_id as merge_key, ranked_updates_books.*
        FROM ranked_updates_books
        
        UNION ALL

        SELECT NULL as merge_key, ranked_updates_books.*
        FROM ranked_updates_books JOIN kafka.books_silver_rowtime
        ON ranked_updates_books.book_id = kafka.books_silver_rowtime.book_id
        WHERE kafka.books_silver_rowtime.current = 1 and ranked_updates_books.price <> kafka.books_silver_rowtime.price
    ) staged_update
    ON kafka.books_silver_rowtime.book_id = merge_key
    WHEN MATCHED AND kafka.books_silver_rowtime.current = 1 AND staged_update.price <> kafka.books_silver_rowtime.price THEN
        UPDATE SET current = 0, end_date = staged_update.updated
    WHEN NOT MATCHED THEN
    INSERT (book_id, title, author, price, current, effective_date, end_date, row_time)
    VALUES (staged_update.book_id, staged_update.title, staged_update.author, staged_update.price, 1, staged_update.updated, NULL, staged_update.row_time)
        """
    microBatchDF.sparkSession.sql(sql_qry)


# COMMAND ----------

from pyspark.sql import functions as F

schema = "book_id STRING, title STRING, author STRING, price DOUBLE, updated TIMESTAMP, row_time TIMESTAMP"

query = (spark.readStream
         .table("kafka.bronze")
         .filter("topic = 'books'")
         .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
         .select("v.*")
         .writeStream
         .foreachBatch(books_upsert)
         .option("checkpointLocation", "/mnt/adobeadls/dwanalytics/orders/kafka/checkpoint/books_rowtime/silver")
         .trigger(availableNow=True)
         .start()
      )
query.awaitTermination()

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
