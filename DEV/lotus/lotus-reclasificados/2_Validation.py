# Databricks notebook source
if (dbutils.fs.ls("/mnt/lotus-reclasificados/")):
  if(dbutils.fs.ls("/mnt/lotus-reclasificados-storage")):
    print("OK")

# COMMAND ----------

dbutils.fs.ls("/mnt/lotus-reclasificados/raw")