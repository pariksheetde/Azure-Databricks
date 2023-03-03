# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def ingest_dtm(input_df):
  output_df = input_df.withColumn("load_ts", current_timestamp())
  return output_df
