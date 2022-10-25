# Databricks notebook source
# DBTITLE 1,Parámetros de los Anexos
"""SrcAplicacion"""
dbutils.widgets.text("SrcAplicacion", "","")
dbutils.widgets.get("SrcAplicacion")
sa = getArgument("SrcAplicacion")

"""CarpetaBlob"""
dbutils.widgets.text("CarpetaBlob", "","")
dbutils.widgets.get("CarpetaBlob")
cb = getArgument("CarpetaBlob")

"""CarpetaNAS"""
dbutils.widgets.text("CarpetaOrigen", "","")
dbutils.widgets.get("CarpetaOrigen")
cn = getArgument("CarpetaOrigen")

"""storageName"""
sn = dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "StorageAccount")

"""sasToken"""
st = dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "SasStorageAccount")

#json_filename = "/mnt/" + sa + "-" + cb + "/raw/" + jf

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

"""DatosLeidos"""
dbutils.widgets.text("datosleidos", "","")
dbutils.widgets.get("datosleidos")
datosleidos = getArgument("datosleidos")

"""DatosEscritos"""
dbutils.widgets.text("datosescritos", "","")
dbutils.widgets.get("datosescritos")
datosescritos = getArgument("datosescritos")

# COMMAND ----------

# DBTITLE 1,Parámetros HTTP Request
"""URL API Rest Table Storage Account"""
url_name = "https://" + sn + ".table.core.windows.net/LotusAnexosAuditoria(PartitionKey='" + str.replace(cb, '/', '.') + "',RowKey='" + str.replace(cn, '/', '.') + "')" + st

"""Body Request"""
data_json = "{'LoadDate': '" + loadDate + "'," + \
            "'Status': '" + status + "'," + \
            "'CopyTime': '" + copyTime + "'," + \
            "'Throughput': '" + throughput + "'," + \
            "'Duration': '" + duration + "'," + \
            "'FilesRead': '" + filesRead + "'," + \
            "'FilesWritten': '" + filesWritten + "'," + \
            "'CarpetaOrigen': '" + cn + "'," + \
            "'DataRead': '" + datosleidos + "'," + \
            "'DataWritten': '" + datosescritos + "'," + \
            "'PartitionKey': '" + str.replace(cb, '/', '.') + "'," + \
            "'RowKey': '" + str.replace(cn, '/', '.') + "'}"

print(data_json)

# COMMAND ----------

def update_entity(url, data):
  import requests
  import json
  import ast
  headers = {'content-type': 'application/json'}
  data = ast.literal_eval(data)
  r = requests.put(url, data=json.dumps(data), headers=headers)
  print(r.status_code)
  if r.status_code != 204:
    raise Exception("Fallo actualizando la entidad (HTTP-PUT): " + jf)


# COMMAND ----------

# DBTITLE 1,Main
update_entity(url_name, data_json) 