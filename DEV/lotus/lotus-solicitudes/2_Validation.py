# Databricks notebook source
if (dbutils.fs.ls("/mnt/lotus-solicitudes/")):
  if(dbutils.fs.ls("/mnt/lotus-solicitudes-storage")):
    print("OK")

# COMMAND ----------

dbutils.fs.ls("/mnt/lotus-solicitudes/raw")