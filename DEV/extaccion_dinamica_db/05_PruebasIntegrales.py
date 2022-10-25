# Databricks notebook source
import os
import pandas as pd
import numpy as np
from pyspark import SQLContext
from os import path, walk, makedirs

# COMMAND ----------

"""---------------------------Parámetros de entrada Info tabla----------------------------"""

# FOLDER BLOB: app-semih.
dbutils.widgets.text("folder_blob", "","")
dbutils.widgets.get("folder_blob")
folder_blob = getArgument("folder_blob")

# APP
application_folder_blob = folder_blob
print(type(application_folder_blob), application_folder_blob)
application_folder_blob = folder_blob.split('/')[0]
print(application_folder_blob)
application = application_folder_blob.split('-')[1]
application = application.strip().upper()
print(application)

# BD AUDITORIA
str_bd_aud = 'AUDITORIA'

# FOLDER INTEGRITY TEST
str_foldert_blob_integrity_test =  application_folder_blob + '-storage' + '/integrity-test'

# TABLA INVENTARIO TABLAS
tbl_inventario =  'Zdr_' + application + '_src_metadatos_inventario_tablas_bd'

# TABLA AUD VALIDACIONES METADATOS (PRE-CARGA)
tbl_aud_validaciones_metadatos =  'Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata'

# TABLA COPY IN BLOB (ZDC)
tbl_aud_copy_in_blob =  'zdr_auditoria_log_extraction_bd_tbl_in_blob'

# TABLA CREATE TABLES IN (ZDR)
tbl_aud_auditoria_log_extraction_bd_tbl_in_data_lake =  'zdr_auditoria_log_extraction_bd_tbl_in_data_lake'

# COMMAND ----------

"""
Descripción: Generar archivo plano a partir de tablas de AUD para las pruenas de integridad.
Autor: Juan David Escobar E.
Fecha creción: 17/11/2020.
"""

def generar_file_aud_tables_integrity_test(prm_query_tbl, prm_file_name):
  
  # Path escritura archivo blob
  path_escritura_blob = str_foldert_blob_integrity_test

  # Path escritura archivo dbfs
  path_escritura_dbfs = "/dbfs/mnt/" + path_escritura_blob + "/" + prm_file_name

  # Eliminar archivo de propiedades existente
  dbutils.fs.rm(path_escritura_dbfs, True)

  # Enable Arrow-based columnar data transfers
  spark.conf.set("spark.sql.execution.arrow.enabled", "true")

  # query de la tabla
  result_query_tbl = spark.sql(prm_query_tbl)

  # Conversion a Pandas Dataframe
  pd_result_query_tbl = result_query_tbl.toPandas()
  pd_result_query_tbl.to_csv(path_escritura_dbfs, index = False, sep='|')

# COMMAND ----------

"""
Descripción: Generar el sud directorio mkdir /dbfs/mnt/app-nombre_app-storage/integrity-test/ en
             caso de que no exista.
Autor: Juan David Escobar E.
Fecha creción: 19/11/2020.
"""

def fn_create_sub_dir_container_app_blob():
  
  # Path escritura archivo blob
  path_escritura_blob = str_foldert_blob_integrity_test

  # Path escritura archivo dbfs escritura en blob (integrity - test)
  path_escritura_dbfs = "/dbfs/mnt/" + path_escritura_blob 
    
  if not path.exists(path_escritura_dbfs):
      makedirs(path_escritura_dbfs)
      print('ruta: {0} creada correctamente.'.format({path_escritura_dbfs}))  

# COMMAND ----------

# DBTITLE 1,INIT
# 0. Generar el sud directorio mkdir /dbfs/mnt/app-nombre_app-storage/integrity-test/ en caso de que no exista.
fn_create_sub_dir_container_app_blob()

# 1. Obtener row_keys de las tablas que van a ejecutar prueba unitaria (IntegrityTest = 'Si')

sqlContext.sql("REFRESH TABLE " + application + '.' + tbl_inventario) 
list_tbl_inventario = sqlContext.sql("SELECT CONCAT(NombreAplicacion, '-', NombreSubAplicacion, '-', NombreTablaAplicacion) as row_key   FROM " +  application + '.' + tbl_inventario  + " WHERE IntegrityTest = 'Si' ").collect()
lista_row_key = ["'" + str(i.row_key) + "'" for i in list_tbl_inventario]  
str_row_keys = ', '.join([str(x) for (i, x) in enumerate(lista_row_key)])

# 2 Generar archivos planos de las tablas de AUD e Inventario 

# TABLA INVENTARIO TABLAS
# BD AUDITORIA
# TABLA AUD VALIDACIONES METADATOS (PRE-CARGA)
# TABLA COPY IN BLOB (ZDC)
# TABLA CREATE TABLES IN (ZDR)

query_tbl_inventario = 'SELECT * FROM ' + application + '.' + tbl_inventario + ' WHERE IntegrityTest = "Si" '
query_tbl_validacion_metadatos = 'SELECT * FROM ' + str_bd_aud + '.' + tbl_aud_validaciones_metadatos + ' WHERE rowKey in (' + str_row_keys + ')'
query_tbl_aud_copy_in_blob = 'SELECT * FROM ' + str_bd_aud + '.' + tbl_aud_copy_in_blob + ' WHERE rowKey in (' + str_row_keys + ')'
query_tbl_aud_create_tbl_data_lake = 'SELECT * FROM ' + str_bd_aud + '.' + tbl_aud_auditoria_log_extraction_bd_tbl_in_data_lake + ' WHERE row_key in (' + str_row_keys + ')'

file_tbl_inventario = tbl_inventario + '.txt'
file_tbl_validacion_metadatos = tbl_aud_validaciones_metadatos + '.txt'
file_tbl_aud_copy_in_blob = tbl_aud_copy_in_blob + '.txt'
file_tbl_aud_create_tbl_data_lake = tbl_aud_auditoria_log_extraction_bd_tbl_in_data_lake + '.txt'

print(file_tbl_inventario)
print(file_tbl_validacion_metadatos)
print(file_tbl_aud_copy_in_blob)
print(file_tbl_aud_create_tbl_data_lake)

generar_file_aud_tables_integrity_test(query_tbl_inventario, file_tbl_inventario.lower())
generar_file_aud_tables_integrity_test(query_tbl_validacion_metadatos, file_tbl_validacion_metadatos.lower())
generar_file_aud_tables_integrity_test(query_tbl_aud_copy_in_blob, file_tbl_aud_copy_in_blob.lower())
generar_file_aud_tables_integrity_test(query_tbl_aud_create_tbl_data_lake, file_tbl_aud_create_tbl_data_lake.lower())

print('ok')

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls /dbfs/mnt/app-semih-storage/integrity-test/
