# Databricks notebook source
# MAGIC %md
# MAGIC # EJECUCION AUDITORIA

# COMMAND ----------

import os

# COMMAND ----------

dbutils.widgets.text("arr_files", "","")
dbutils.widgets.get("arr_files")
arr_files = getArgument("arr_files")

dbutils.widgets.text("application", "","")
dbutils.widgets.get("application")
application = getArgument("application")

# COMMAND ----------

arr_files = list(arr_files.split(",")) 
for file in arr_files:
  print(file)
  path_file = "/dbfs/mnt/dbbicorporativo-indicadores/raw/" + file
  try:
    print("origen: {0}, path_file:{1}, file_name:{2}, CarpetaBlob:{3}".format("DB", path_file, file, application))
    dbutils.notebook.run("/dbbicorporativo/dbbicorporativo-indicadores/3_CreateSchema", 72000, {"origen": "DB",
                                                                                                "path_file":path_file,
                                                                                                "file_name":file,
                                                                                                "CarpetaBlob":application})
  except Exception as error:
    print(error, "\n Error al procesar el archivo" + file)
