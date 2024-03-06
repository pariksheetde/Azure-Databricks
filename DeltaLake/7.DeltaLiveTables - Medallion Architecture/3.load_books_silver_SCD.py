# Databricks notebook source
# MAGIC %md
# MAGIC #### LOAD ORDERS DATA FROM MULTIPLEX BRONZE TABLE INTO KAFKA.BOOKS_SILVER TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS kafka.books_silver
# MAGIC (
# MAGIC   book_id STRING,
# MAGIC   title STRING,
# MAGIC   author STRING,
# MAGIC   price DOUBLE,
# MAGIC   current INT,
# MAGIC   effective_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP
# MAGIC )

# COMMAND ----------

def scd_books(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("updates")

    sql_qry = """
    MERGE INTO kafka.books_silver
    USING(
        SELECT updates.book_id as merge_key, updates.*
        FROM updates
        
        UNION ALL

        SELECT NULL as merge_key, updates.*
        FROM updates JOIN kafka.books_silver
        ON updates.book_id = kafka.books_silver.book_id
        WHERE kafka.books_silver.current = 1 and updates.price <> kafka.books_silver.price
    ) staged_update
    ON kafka.books_silver.book_id = merge_key
    WHEN MATCHED AND kafka.books_silver.current = 1 and staged_update.price <> kafka.books_silver.price THEN
        UPDATE SET current = 0, end_date = staged_update.updated
    WHEN NOT MATCHED THEN
    INSERT (book_id, title, author, price, current, effective_date, end_date)
    VALUES (staged_update.book_id, staged_update.title, staged_update.author, staged_update.price, 1, staged_update.updated, NULL)
    """
    # spark.sql(sql_qry)
    microBatchDF.sparkSession.sql(sql_qry)


# COMMAND ----------

def process_books():
   from pyspark.sql import functions as F
   books_schema = "book_id STRING, title STRING, author STRING, price DOUBLE, updated TIMESTAMP"

   qry = (spark.readStream.table("kafka.bronze")
         .filter("topic = 'books'")
         .select(F.from_json(F.col("value").cast("string"), books_schema).alias("v"))
         .select("V.*")
         .writeStream
         .foreachBatch(scd_books)
         .option("checkpointLocation", "/mnt/adobeadls/dwanalytics/orders/kafka/checkpoint/books/silver")
         # .trigger(processingTime='5 seconds')
         .trigger(availableNow=True)
         .start()
      )
   qry.awaitTermination()

# COMMAND ----------

process_books()
