# Databricks notebook source
import os

# COMMAND ----------

"""Path Ruta de la Aplicacion"""
dbutils.widgets.text("Path", "","")
dbutils.widgets.get("Path")
Path = getArgument("Path")

name_archive = 'execute_json_auditoria.txt'
source = '/dbfs'
source_mount = '/mnt/lotus-'
aplication = [Path[Path.index("-") + 1:]]

# COMMAND ----------

for aplication in aplication:
  for json in os.listdir(source + source_mount + aplication + '/raw'):
    try:
      dbutils.notebook.run("/auditoria/Soporte_Auditoria", 72000, {"json_name": json, "aplication":aplication})
    except Exception as error:
        print(error, "\n Error al procesar el JSON" + json)
