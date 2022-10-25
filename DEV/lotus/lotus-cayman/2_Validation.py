# Databricks notebook source
if (dbutils.fs.ls("/mnt/lotus-cayman/")):
  if(dbutils.fs.ls("/mnt/lotus-cayman-storage")):
    print("OK")

# COMMAND ----------

dbutils.fs.ls("/mnt/lotus-cayman/raw")