# Databricks notebook source
if (dbutils.fs.ls("/mnt/lotus-pedidos/")):
  if(dbutils.fs.ls("/mnt/lotus-pedidos-storage")):
    print("OK")

# COMMAND ----------

dbutils.fs.ls("/mnt/lotus-pedidos/pedidos/raw")


# COMMAND ----------

dbutils.fs.ls("/mnt/lotus-pedidos/subpedidos/raw")