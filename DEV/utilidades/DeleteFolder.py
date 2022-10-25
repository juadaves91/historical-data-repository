# Databricks notebook source
# DBTITLE 1,Variables Globales
"""Variables Staticas"""
MOUNT_POINT_UNIT = '/mnt/'
FILE_SYSTEM = '/dbfs'
FOLDER = "/results/"

"""Path Ruta de la Aplicacion"""
Path = getArgument("Path")



# COMMAND ----------

import os 

"""Eliminar Zona de Resultados"""

def Delete_Files():
  try:
    for Files in os.listdir(FILE_SYSTEM + MOUNT_POINT_UNIT + Path + FOLDER):
      dbutils.fs.rm(MOUNT_POINT_UNIT + Path + FOLDER + Files, True)
  except Exception as error:
    return error

Delete_Files()