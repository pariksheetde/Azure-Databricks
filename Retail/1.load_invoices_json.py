# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM JSON.`/mnt/adobeadls/retail/invoices`

# COMMAND ----------

:from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType
class invoiceStream():
    def __init__():
        self.base_dir = "/mnt/adobeadls/retail/invoices"

    def getInvoiceSchema(self):
        return """InvoiceNumber STRING, CreatedTime BIGINT, StoreID STRING, PosID STRING, CashierID STRING,
                CustomerType STRING, CustomerCardNo STRING, TotalAmount DOUBLE, NumberOfItems BIGINT,
                PaymentMethod STRING, TaxableAmount DOUBLE, CGST DOUBLE, SGST DOUBLE, CESS DOUBLE, DeliveryType STRING,
                DeliveryAddress STRUCT<AddressLine STRING, City STRING, ContactNumber STRING, PinCode STRING, State STRING>
                InvoiceLineItems ARRAY<STRUCT<ItemCode STRING, ItemDescription STRING, ItemPrice DOUBLE, ItemQty BIGINT, TotalValue DOUBLE>>
        """
    
    def readInvoices(self):
        return (spark.readStream 
                     .format("json")
                     .schema(self.getInvoiceSchema())
                     .load("{self.base_dir}")
                     )
    
    def explodeInvoices(self, InvoiceDF):
        return (InvoiceDF.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID", "CashierID", "CustomerType", "CustomerCardNo", 
                                     "TotalAmount", "NumberOfItems", "PaymentMethod", "TaxableAmount", "CGST", "SGST", "CESS", "DeliveryType", 
                                     "DeliveryAddress.AddressLine", "DeliveryAddress.City", "DeliveryAddress.ContactNumber", "DeliveryAddress.PinCode", "DeliveryAddress.State", explode(InvoiceLineItems) as LineItem)
                
                )
        
    def flattenLineItem(self, explodeDF):
        from pyspark.sql.functions import expr
        return (explodeDF.withColumn("ItemCode", expr("LineItem.ItemCode"))
                         .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
                         .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
                         .withColumn("ItemQty", expr("LineItem.ItemQty"))
                         .withColumn("TotalValue", expr("LineItem.TotalValue"))
                         .drop("LineItem")
        )

    def loadInvoices(self, flattenDF):
        return (flattenDF.writeStream
                         .format("delta")
                         .option("checkpointLocation", "/mnt/adobeadls/retail/checkpoint/invoices")
                         .outputMode("append")
                         .table("retail.invoices")
        )

    def main(self):
        print(f"Starting Invoice Processing....")
        invoiceDF = self.readInvoices()
        explodeDF = self.explodeInvoices(invoiceDF)
        flattenDF = self.flattenLineItem(explodeDF)
