# Databricks notebook source
# DBTITLE 1,Varaibles de entrada
"""SrcAplicacion"""
dbutils.widgets.text("SrcAplicacion", "","")
dbutils.widgets.get("SrcAplicacion")
SrcAplicacion = getArgument("SrcAplicacion")

"""CarpetaBlob"""
dbutils.widgets.text("CarpetaBlob", "","")
dbutils.widgets.get("CarpetaBlob")
CarpetaBlob = getArgument("CarpetaBlob")

"""FolderBlob EJ: app-semih"""
dbutils.widgets.text("FolderBlob", "","")
dbutils.widgets.get("FolderBlob")
FolderBlob = getArgument("FolderBlob")

"""IsExecutionFromEDinamicaBD: Valor boolean que determina si la ejecución del Nootbook BCK proviene de 
la ETL ExtraccionDinamicaBD ya que en este caso no se debe ejecutar los metodos de los comandos posteriores a este"""
dbutils.widgets.text("IsExecutionFromEDinamicaBD", "","")
dbutils.widgets.get("IsExecutionFromEDinamicaBD")
IsExecutionFromEDinamicaBD = eval(getArgument("IsExecutionFromEDinamicaBD"))


print('SrcAplicacion: ', SrcAplicacion)
print('CarpetaBlob: ', CarpetaBlob)
print('IsExecutionFromEDinamicaBD: ', IsExecutionFromEDinamicaBD)
print('CMD1')

# COMMAND ----------

# DBTITLE 1,Variables Globales
"""
Descripcion: Inicialización de variables globales.
Fecha: 18/06/2020
"""

if not IsExecutionFromEDinamicaBD:

  path_root = '/dbfs'
  path_mnt = '/mnt/'
  path_root_bck = '/bck-data-lake'
  path_zdr = '/results'
  str_storage = 'storage'
  subapplication = 'N/A'
  path_subapplication = ''

else:
    
  application_folder_blob = FolderBlob #app-semih/metadatos
  application_folder_blob = FolderBlob.split('/')[0] #app-semih
  application_folder_blob = application_folder_blob.split('-') #[app,semih]
  
  SrcAplicacion = application_folder_blob[0] #app
  CarpetaBlob = application_folder_blob[1] #semih
  
  print("SrcAplicacion", SrcAplicacion)
  print("CarpetaBlob", CarpetaBlob)
  
print('CMD2')

# COMMAND ----------

# DBTITLE 1,Identificación App y Subapp
if not IsExecutionFromEDinamicaBD:

  CarpetaBlobAux = CarpetaBlob.split("/")
  print(CarpetaBlobAux)
  application = CarpetaBlobAux[0].strip()

  if len(CarpetaBlobAux) > 1:
    subapplication = CarpetaBlobAux[1].strip()
    path_subapplication = '/' + subapplication

print('CMD3')

# COMMAND ----------

# DBTITLE 1,Call Generic Funtions To Execute Bck Datalake to Blob
# MAGIC %run /utilidades/PTR/HelperBckDataLakeToBlob

# COMMAND ----------

# DBTITLE 1,Init args
"""
Ejemplo:

-path_sink = '/mnt/lotus-cayman-storage/bck-data-lake'
-path_app_data_lake = '/dbfs/mnt/lotus-cayman'
-prm_path_sink = '/dbfs/mnt/lotus-cayman-storage'
-path_src = '/dbfs/mnt/lotus-cayman-storage'
"""

if not IsExecutionFromEDinamicaBD:
  
  path_sink = path_mnt + SrcAplicacion + '-' + application + '-' + str_storage + path_subapplication + path_root_bck
  path_app_data_lake = path_root + path_mnt + SrcAplicacion + '-' + application + path_subapplication
  prm_path_sink = path_root + path_mnt + SrcAplicacion + '-' + application + '-' + str_storage
  path_src = path_root + path_mnt + SrcAplicacion + '-' + application + '-' + str_storage + path_subapplication

print('CMD5')

# COMMAND ----------

# DBTITLE 1,Init args and generate control files list (Files in Raw/Error)
if not IsExecutionFromEDinamicaBD:

  path_src = path_root + path_mnt + SrcAplicacion + '-' + application + '-' + str_storage + path_subapplication

  prm_dict_arg_create_file = {
    'path_src': path_src,
    'path_sink': path_root + path_sink,
    'list_headers': ['application', 'subapplication', 'file', 'folder', 'date']
  }
  
  createFile(prm_dict_arg_create_file)

  prm_dict_params = {
    'application': application,
    'subapplication': subapplication,
    'path_root_db': path_root,
    'path_sink': path_sink,
    'filter_col': 'file',
    'path_app_data_lake': path_app_data_lake
  }

  print(prm_dict_params)
  updateControlFile(prm_dict_params, 'raw')
  updateControlFile(prm_dict_params, 'error')
  print('CMD6')

# COMMAND ----------

# DBTITLE 1,Execute Copy Bck Files From Datalake to Blob Storage Apps (Folder:Result)
"""
Este método se encuentran comentados debido a que no se estan utilizando, dichos metodos fue remplazado
por tareas de copy en el pipeline PTR > Exe:bck
"""

# path_datalake = SrcAplicacion + '-' + application + path_subapplication + path_zdr
# path_folder_results_data_lk = path_mnt + path_datalake
# path_folder_results_blob = path_mnt + SrcAplicacion + '-'  + application + '-' + str_storage + path_subapplication + path_root_bck + path_datalake

# args_dic_bck = {
#                  'path_root':path_root,
#                  'path_folder_results_data_lk': path_folder_results_data_lk,
#                  'path_folder_results_blob': path_folder_results_blob
#                }
# copyBck(args_dic_bck)
# print('CMD7')

# COMMAND ----------

# DBTITLE 1,Execute Copy Bck Files From Datalake to Blob Storage Auditoria(Folder:Result)
"""
Este método se encuentran comentados debido a que no se estan utilizando, dichos metodos fue remplazado
por tareas de copy en el pipeline PTR > Exe:bck
"""

# path_datalake = 'rdh-auditoria/results'
# path_folder_results_data_lk = path_mnt + path_datalake
# path_folder_results_blob = '/mnt/rdh-auditoria-storage' + path_root_bck + path_datalake

# args_dic_bck = {
#                  'path_root':path_root,
#                  'path_folder_results_data_lk': path_folder_results_data_lk,
#                  'path_folder_results_blob': path_folder_results_blob
#                }

# copyBck(args_dic_bck)
# print('CMD8')

# COMMAND ----------

# MAGIC %md
# MAGIC ### RETORNO - OUTPUT NOTEBOOK

# COMMAND ----------

"""
Descripción: Retorna valor de los parametros requeridos para la copia de la ZDR - DL hacia el contenedor
de cada aplicación en el Blob Storage.
Fecha: 04/01/2021.
Autor: Juan Escobar.
"""

import json

print("SrcAplicacion", SrcAplicacion)
print("CarpetaBlob", CarpetaBlob)

dbutils.notebook.exit(json.dumps({
  "SrcAplicacion": SrcAplicacion,
  "CarpetaBlob": CarpetaBlob
}))
