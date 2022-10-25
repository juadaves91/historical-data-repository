# Databricks notebook source
if (dbutils.fs.ls("/mnt/lotus-requerimientos-legales/")):
  if(dbutils.fs.ls("/mnt/lotus-requerimientos-legales-storage")):
    print("OK")

# COMMAND ----------

dbutils.fs.ls("/mnt/lotus-requerimientos-legales/raw")