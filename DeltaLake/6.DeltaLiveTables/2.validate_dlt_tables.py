# Databricks notebook source
files = dbutils.fs.ls("/mnt/adobeadls/dwanalytics/orders/logs")
display(files)
