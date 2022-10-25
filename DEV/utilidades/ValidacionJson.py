# Databricks notebook source
import pandas as pd
import os
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,Parámetros de Entrada
"""Archivo JSON a validar"""
dbutils.widgets.text("json_file", "","")
dbutils.widgets.get("json_file")
jf = getArgument("json_file")

"""SrcAplicacion"""
dbutils.widgets.text("SrcAplicacion", "","")
dbutils.widgets.get("SrcAplicacion")
sa = getArgument("SrcAplicacion")

"""CarpetaBlob"""
dbutils.widgets.text("CarpetaBlob", "","")
dbutils.widgets.get("CarpetaBlob")
cb = getArgument("CarpetaBlob")

"""storageName"""
sn = dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "StorageAccount")

"""sasToken"""
st = dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "SasStorageAccount")

"""json file"""
json_filename = "/mnt/" + sa + "-" + cb + "/raw/" + jf
print(json_filename)

"""json file corrupt"""
json_filename_corrupt = "/mnt/" + sa + "-" + cb + "/error/" + jf
print(json_filename_corrupt)

# COMMAND ----------

# DBTITLE 1,Parámetros del Resultado de Ejecución
from datetime import datetime
loadDate = datetime.now().strftime('%Y%m%d %H:%M:%S')

"""Status"""
dbutils.widgets.text("Status", "","")
dbutils.widgets.get("Status")
status = getArgument("Status")

"""CopyTime"""
dbutils.widgets.text("CopyTime", "","")
dbutils.widgets.get("CopyTime")
copyTime = getArgument("CopyTime")

"""Throughput"""
dbutils.widgets.text("Throughput", "","")
dbutils.widgets.get("Throughput")
throughput = getArgument("Throughput")

"""Duration"""
dbutils.widgets.text("Duration", "","")
dbutils.widgets.get("Duration")
duration = getArgument("Duration")

"""FilesRead"""
dbutils.widgets.text("FilesRead", "","")
dbutils.widgets.get("FilesRead")
filesRead = getArgument("FilesRead")

"""FilesWritten"""
dbutils.widgets.text("FilesWritten", "","")
dbutils.widgets.get("FilesWritten")
filesWritten = getArgument("FilesWritten")

"""CarpetaOrigen"""
dbutils.widgets.text("CarpetaOrigen", "","")
dbutils.widgets.get("CarpetaOrigen")
carpetaOrigen = getArgument("CarpetaOrigen")

"""MD5"""
dbutils.widgets.text("md5", "","")
dbutils.widgets.get("md5")
md5 = getArgument("md5")

"""Registros"""
dbutils.widgets.text("registros", "","")
dbutils.widgets.get("registros")
registros = getArgument("registros")

"""DatosLeidos"""
dbutils.widgets.text("datosleidos", "","")
dbutils.widgets.get("datosleidos")
datosleidos = getArgument("datosleidos")

"""DatosEscritos"""
dbutils.widgets.text("datosescritos", "","")
dbutils.widgets.get("datosescritos")
datosescritos = getArgument("datosescritos")

# COMMAND ----------

"""
Descripción: valida los registros corruptos de un json
Fecha Modificación: 13/02/2020
Autor: Blaimir Ospina
"""

def fn_corrupt_records(json_dataframe):
  error = ""
  if "_corrupt_record" in json_dataframe.columns and int(json_dataframe.filter(col("UniversalID").isNull()).count()) > 0:
      print("error: registro invalido")
      error="UniversalID invalidos: "
      malos = json_dataframe.filter(col("UniversalId").isNull()).toPandas()
      lista_malos = malos["_corrupt_record"].tolist()
      for i in range(len(lista_malos)):
        Universalid = lista_malos[i].split(",")
        Universalid = Universalid[0].split(":")
        error += "["+(Universalid[1].replace('"',''))+"] "
      
      dbutils.fs.mv(json_filename,json_filename_corrupt) 
      return error
  else:
    if os.path.exists("/dbfs" + json_filename_corrupt):
      dbutils.fs.rm(json_filename_corrupt)
    print("Json ok")
  return error

# COMMAND ----------

# MAGIC %run /utilidades/SchemaApp

# COMMAND ----------

def ft_schem_json(json_filename):
  try:
    error = ""
    aplication = str.replace(cb, '/', '.')
    if aplication == "cayman":
      json_dataframe = spark.read.schema(ft_schema_comex().schema).json(json_filename)
      error = fn_corrupt_records(json_dataframe)
    if aplication == "comex":
      json_dataframe = spark.read.schema(ft_schema_comex().schema).json(json_filename)
      error = fn_corrupt_records(json_dataframe)
    if aplication == "pedidos.pedidos":
      json_dataframe = spark.read.schema(ft_schema_pedidos().schema).json(json_filename)
      error = fn_corrupt_records(json_dataframe)
    if aplication == "pedidos.subpedidos":
      json_dataframe = spark.read.schema(ft_schema_subpedidos().schema).json(json_filename)
      error = fn_corrupt_records(json_dataframe)
    if aplication == "reclamos.primercontacto":
      json_dataframe = spark.read.schema(ft_schema_primercontacto().schema).json(json_filename)
      error = fn_corrupt_records(json_dataframe)
    if aplication == "reclamos.primercontacto-old":
      json_dataframe = spark.read.schema(ft_schema_primercontacto_old().schema).json(json_filename)
      error = fn_corrupt_records(json_dataframe)
    if aplication == "reclamos.reclamos":
      json_dataframe = spark.read.schema(ft_schema_reclamos().schema).json(json_filename)
      error = fn_corrupt_records(json_dataframe)
    if aplication == "reclamos.reclamos-old":
      json_dataframe = spark.read.schema(ft_schema_reclamos_old().schema).json(json_filename)
      error = fn_corrupt_records(json_dataframe)
    if aplication == "reclamos.subreclamos":
      json_dataframe = spark.read.schema(ft_schema_subreclamos().schema).json(json_filename)
      error = fn_corrupt_records(json_dataframe)
    if aplication == "reclamos.subreclamos-old":
      jsonerror_dataframe = spark.read.schema(ft_schema_subreclamos_old().schema).json(json_filename)
      error = fn_corrupt_records(json_dataframe)
    if aplication == "reclasificados":
      json_dataframe = spark.read.schema(ft_schema_reclasificados().schema).json(json_filename)
      error = fn_corrupt_records(json_dataframe)
    if aplication == "requerimientos-legales":
      json_dataframe = spark.read.schema(ft_schema_req_legales().schema).json(json_filename)
      error = fn_corrupt_records(json_dataframe)
    if aplication == "solicitudes":
      json_dataframe = spark.read.schema(ft_schema_solicitudes().schema).json(json_filename)
      error = fn_corrupt_records(json_dataframe)
      
    print("Inferir Esquema ok")  
    return error
  
  except Exception as error:
    dbutils.fs.mv(json_filename,json_filename_corrupt)
    print("No pudo Inferir Esquema")
    return error

# COMMAND ----------

# DBTITLE 1,Función: validación del JSON File

def validate_json(json_filename):
  import pandas as pd
  #import numpy as np
  #from pyspark.sql.functions import col
  #import ast
  try:
    error=""    
    json_dataframe = spark.read.json(json_filename)
    error = fn_corrupt_records(json_dataframe)
    print(error)
    return error
  except Exception as error:
    error = ft_schem_json(json_filename)
    return error

# COMMAND ----------

# DBTITLE 1,Función: actualización de table storage
def json_error(url, data):
  import requests
  import json
  import ast
  headers = {'content-type': 'application/json'}
  data = ast.literal_eval(data)
  r = requests.put(url, data=json.dumps(data), headers=headers)
  print(r.status_code)
  if r.status_code != 204:
    raise Exception("Fallo actualizando la entidad (HTTP-PUT): " + jf)
  return r.status_code

# COMMAND ----------

# DBTITLE 1,Si existe error en la validación del json, construye el http request y actualiza el table storage
error = str(validate_json(json_filename))

if (error != "None" and error != ""):
  """URL API Rest Table Storage Account"""
  url_name = "https://" + sn + ".table.core.windows.net/LotusJson(PartitionKey='" + str.replace(cb, '/', '.') + "',RowKey='" + jf + "')" + st

  """Body Request"""
  data_json = '{"LoadDate": "' + loadDate + '",' + \
            '"Status": "' + status + '",' + \
            '"CopyTime": "' + copyTime + '",' + \
            '"Throughput": "' + throughput + '",' + \
            '"Duration": "' + duration + '",' + \
            '"FilesRead": "' + filesRead + '",' + \
            '"FilesWritten": "' + filesWritten + '",' + \
            '"CarpetaOrigen": "' + carpetaOrigen + '",' + \
            '"md5": "' + md5 + '",' + \
            '"registros": "' + registros + '",' + \
            '"DataRead": "' + datosleidos + '",' + \
            '"DataWritten": "' + datosescritos + '",' + \
            '"Error": "' + error + '",' + \
            '"PartitionKey": "' + str.replace(cb, '/', '.') + '",' + \
            '"RowKey": "' + jf + '"}'
  
  print(data_json)
  json_error(url_name, data_json)
