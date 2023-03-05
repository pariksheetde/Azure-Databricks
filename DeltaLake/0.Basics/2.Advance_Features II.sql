-- Databricks notebook source
OPTIMIZE delta.locations
ZORDER BY loc_id;

-- COMMAND ----------


