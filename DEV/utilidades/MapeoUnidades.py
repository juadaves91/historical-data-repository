# Databricks notebook source
'''
Descripcion: Parametros de entrada enviados desde el Datafactory para la creación o eliminación de unidades virtuales (Mapeo de unidades)
Autor: Juan Escobar, Blaimir Ospina
Fecha: 08/05/2020
Ej Ejecuciones, combinacion de parametros:

* ArgDicMapeoUnidades =

{'applications_storage':[{'nombre_unidad_virtual': '','contenedor':''}]}

{'applications_data_lake':[{'nombre_unidad_virtual': '','contenedor':''}]}

{'applications_storage':[
                         {
                          'nombre_unidad_virtual': 'lotus-cayman-storage', 
                          'contenedor': 'lotus-cayman'
                         }
                        ]
}

{'applications_data_lake':[
                            {
                               'nombre_unidad_virtual':'lotus-cayman',                                           
                               'key_name':'usernamecayman', 
                               'key_pwd':'passwordcayman'
                            }
                          ]
}

TipoModificacion: "creacion", "eliminacion", "consulta"
'''

# COMMAND ----------

# DBTITLE 1,Import Libraries
import sys
import os
import ast 
from datetime import datetime
import pytz
from pyspark import SQLContext
import pandas as pd
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1, Parametros de Entrada

"""Lista de Mapeo de Unidades"""

dbutils.widgets.text("ArgDicMapeoUnidades", "","")
dbutils.widgets.get("ArgDicMapeoUnidades")
dic_mount_points = getArgument("ArgDicMapeoUnidades")

"""Valor de Creacion/Eliminacion/Consulta Mapeo de Unidades"""

dbutils.widgets.dropdown("TipoModificacion", "creacion", ["creacion", "eliminacion", "consulta"])
dbutils.widgets.get("TipoModificacion")
tipo_modificacion = getArgument("TipoModificacion")

# COMMAND ----------

# Format str to dic and cast
dic_mount_points = ''.join(dic_mount_points.split())
dic_mount_points = dic_mount_points.replace('\\', '')
dic_mount_points = ast.literal_eval(dic_mount_points)

# COMMAND ----------

# DBTITLE 1,Definición Variables Globales
#Variables globales Mapeo de unidades data lake

SOURCE = "adl://historicosdesadla01.azuredatalakestore.net/"

#Variables globales Mapeo de unidades Blob Storage

SOURCE_PROTOCOL = 'wasbs://'
SOURCE_URL_SA = '@historicosdesasac01.blob.core.windows.net' 
MOUNT_POINT_UNIT = '/mnt/'
FILE_SYSTEM = '/dbfs'
SCOPE = "historicos-desa-kva-01"
KEY = "SecretStorageAccountKey"
DIC_EXTRA_CONFIGS = {"fs.azure.account.key.historicosdesasac01.blob.core.windows.net": dbutils.secrets.get(scope = SCOPE, key = KEY)}

path_aud_mu_bs = '/mnt/rdh-auditoria/results/Panel_Auditoria_Log_Mapeo_Unidades_Blob_Storage/'
tbl_aud_mu_bs = 'auditoria.Zdr_Auditoria_Log_Mapeo_Unidades_Blob_Storage'

path_aud_mu_dla = '/mnt/rdh-auditoria/results/Panel_Auditoria_Log_Mapeo_Unidades_DataLake/'
tbl_aud_mu_dla = 'auditoria.Zdr_Auditoria_Log_Mapeo_Unidades_DataLake'

# COMMAND ----------

# DBTITLE 1,Init Mapeo unidades - Blob Storage
"""
Descripción: Inicializa el montaje de unidades virtuales entre Databricks y Blob Strorage a partir de la tabla
             que registra el log de auditoria de las unidades creadas ó eliminadas (activas/inactivas) en el trascurso del 
             tiempo para la solución de RDH.
Autor: Juan Escobar, Blaimir Ospina.
Fecha: 02/05/2020.             
"""

def init_mapeo_unidades_blob_storage():
  
  # Debe existir la tabla de log aud previamente..
  if os.path.exists(FILE_SYSTEM + path_aud_mu_bs):
  
    df_aud_log_file = sqlContext.sql('SELECT * FROM ' + tbl_aud_mu_bs)
    pd_df_aud_log_file = df_aud_log_file.toPandas()

    for index, row in pd_df_aud_log_file.iterrows():

      arg_dic_mount_points = {'applications_storage':[  
                                                      {
                                                        'nombre_unidad_virtual': row['nombre_unidad_virtual'], 
                                                        'contenedor': row['contenedor']
                                                      } 
                                                     ]
                             }
      print(type(arg_dic_mount_points), arg_dic_mount_points)  
      fn_mapeo_unidades_storage_account(arg_dic_mount_points)

# COMMAND ----------

# DBTITLE 1,Mapeo Unidades Datalake 
def fn_mapeo_unidades_data_lake(arg_dic_mount_points, isRdhAud = False):
  
  msg_return = 'No se encontraron nuevas unidades de montaje para el Data Lake'
  str_acum_mounted_units = 'Unidades montadas del Data Lake: '
 
  for point in arg_dic_mount_points["applications_data_lake"]:
    if not os.path.exists(FILE_SYSTEM + MOUNT_POINT_UNIT + point['nombre_unidad_virtual']):
      try:
        configs = {
          "dfs.adls.oauth2.access.token.provider.type" : "ClientCredential",
          "dfs.adls.oauth2.client.id" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = point['key_name']),
          "dfs.adls.oauth2.credential" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = point['key_pwd']),
          "dfs.adls.oauth2.refresh.url" : "https://login.microsoftonline.com/428f4e2e-13bf-4884-b364-02ef9af41a1d/oauth2/token"
        }
        dbutils.fs.mount(
          source = SOURCE + point['nombre_unidad_virtual'],
          mount_point = MOUNT_POINT_UNIT + point['nombre_unidad_virtual'],
          extra_configs = configs
        )

#Validar las suguientes dos lineas por que estan evitando seguir la ejecucion
#         str_acum_mounted_units = str_acum_mounted_units + arg_dic_mount_points['nombre_unidad_virtual'] + '\n'
#         msg_return = str_acum_mounted_units

        if isRdhAud == False:
          print(isRdhAud)
          update_panel_auditoria_log_mapeo_unidades_DataLake(point['nombre_unidad_virtual'], 
                                                             point['key_name'], 
                                                             point['key_pwd'])



      except Exception:
        msg_return = sys.exc_info()         

  return msg_return

# COMMAND ----------

# DBTITLE 1,Init Mapeo unidades - Data Lake
"""
Descripción: Inicializa el montaje de unidades virtuales entre Databricks y Datalake a partir de la tabla
             que registra el log de auditoria de las unidades creadas ó eliminadas (activas/inactivas) en el trascurso del 
             tiempo para la solución de RDH.
Autor: Juan Escobar, Blaimir Ospina.
Fecha: 02/05/2020.             
"""

def init_mapeo_unidades_data_lake():
  
  # Debe existir la tabla de log aud previamente..
  if os.path.exists(FILE_SYSTEM + path_aud_mu_dla):
    
    df_aud_log_file = sqlContext.sql('SELECT * FROM ' + tbl_aud_mu_dla)
    pd_df_aud_log_file = df_aud_log_file.toPandas()

    for index, row in pd_df_aud_log_file.iterrows():
      arg_dic_mount_points = {'applications_data_lake':[  
                                                      {
                                                        'nombre_unidad_virtual': row['nombre_unidad_virtual'], 
                                                        'key_name': row['key_name'],
                                                        'key_pwd': row['key_pwd']
                                                      } 
                                                     ]
                             }
    print(arg_dic_mount_points)  
    fn_mapeo_unidades_data_lake(arg_dic_mount_points)

# COMMAND ----------

# DBTITLE 1,Mapeo Unidades Storage Account
def fn_mapeo_unidades_storage_account(dic_mount_points):
  
  # valida si la tabla de auditoria existe previo al registro del log
  
  msg_return = 'No se encontraron nuevas unidades de montaje para el Storage Account'
  str_acum_mounted_units = 'Unidades montadas del Blob Storage: '
 
  for point in dic_mount_points['applications_storage']:
      try:
        print(FILE_SYSTEM + MOUNT_POINT_UNIT + point['nombre_unidad_virtual'])
        if not os.path.exists(FILE_SYSTEM + MOUNT_POINT_UNIT + point['nombre_unidad_virtual']):
         
          dbutils.fs.mount(source = SOURCE_PROTOCOL +point['contenedor'] + SOURCE_URL_SA,
                           mount_point = MOUNT_POINT_UNIT + point['nombre_unidad_virtual'],
                           extra_configs = DIC_EXTRA_CONFIGS) 
#Validar las suguientes dos lineas por que estan evitando seguir la ejecucion         
#           str_acum_mounted_units = str_acum_mounted_units + point['nombre_unidad_virtual'] + '\n'
#           msg_return = str_acum_mounted_units
          
          #Regitrar log en tabla Zdr_Auditoria_Log_Mapeo_Unidades_Blob_Storage
          update_panel_auditoria_log_mapeo_unidades_blob_storage(point['nombre_unidad_virtual'], point['contenedor'])
          
      except Exception:
          msg_return = sys.exc_info() 
          
  return msg_return    

# COMMAND ----------

# DBTITLE 1,Init Rdh Auditoria
def init_aud_mapeo_unidades():
  
#   1.Crear o montar la unidad de rdh-auditoria
  zone = pytz.timezone('America/Bogota')
  date = datetime.now(zone)
  current_date = date.strftime("%d/%m/%Y %H:%M:%S")
                            
  dic_init_aud_la_mu =   "{'applications_data_lake': \
                                           [  \
                                              { \
                                                  'nombre_unidad_virtual': 'rdh-auditoria', \
                                                  'key_name': 'usernamerdhauditoria', \
                                                  'key_pwd': 'passwordrdhauditoria' \
                                              } \
                                            ] \
                                      }"
  
  dic_mount_points_aud = ''.join(dic_init_aud_la_mu.split())
  dic_mount_points_aud = dic_mount_points_aud.replace('\\', '')
  dic_mount_points_aud = ast.literal_eval(dic_mount_points_aud)

  #la priemara vez que se ejecuta la auditoria, el siempre creado el montaje de rdh-auditoria para alamacenar los archivos .parquet de las las unidades de las aplicaciones 
  fn_mapeo_unidades_data_lake(dic_mount_points_aud, True)
  
  # 2. Crear la metadata de la tabla de aud log data lake mu
  
  if not os.path.exists(FILE_SYSTEM + path_aud_mu_dla):
    
    # 2.1 Creo la tabla por primera vez vacia para la aud del datalake
    sqlContext.sql("CREATE DATABASE IF NOT EXISTS auditoria")
    list_cols_aud_mu_dl = ['nombre_unidad_virtual', 'key_name', 'key_pwd', 'Fecha']
    df_aud_mu_dl = spark.createDataFrame([('rdh-auditoria', 'usernamerdhauditoria', 'passwordrdhauditoria', current_date)], list_cols_aud_mu_dl)
    df_aud_mu_dl.write.format('delta').mode('overwrite').option('path', path_aud_mu_dla).saveAsTable(tbl_aud_mu_dla)
    
   
  # 3. Crear la metadata de la tabla de aud log blob mu
  if not os.path.exists(FILE_SYSTEM + path_aud_mu_bs):

    # 3.1 Creo la tabla por primera vez vacia para la aud del blob
    sqlContext.sql("CREATE DATABASE IF NOT EXISTS auditoria")
    list_cols_aud_mu_bs = ['nombre_unidad_virtual', 'contenedor', 'Fecha']
    df_aud_mu_bs = spark.createDataFrame([('','','')], list_cols_aud_mu_bs)
    df_aud_mu_bs = df_aud_mu_bs.filter(col("nombre_unidad_virtual") != '')
    df_aud_mu_bs.write.format('delta').mode('overwrite').option('path', path_aud_mu_bs).saveAsTable(tbl_aud_mu_bs)

# COMMAND ----------

# DBTITLE 1,Actualizar Tabla Log Aud Mapeo Unidades Blob Storage
"""
Descripción: Esta función valida la previa existencia del registro de una unidad virtual creada entre el componente 
             Azure Databricks y Azure Blob Storage, en caso de que el registro no exista se adiciona un nuevo registro
             en la tabla del log de auditoria auditoria.Zdr_Auditoria_Log_Mapeo_Unidades_Blob_Storage.
Autor: Juan Escobar E, Blaimir Ospina C.
Parametros: nombre_unidad_virtual (str), contenedor blob (str).
Ej Ejecución: update_panel_auditoria_log_mapeo_unidades_blob_storage('lotus-pruebas-unitarias-databricks-storage', 'lotus-pruebas-unitarias-databricks').
Fecha: 02/06/2020.
"""

def update_panel_auditoria_log_mapeo_unidades_blob_storage(nombre_unidad_virtual, contenedor):
  
  df_aud_log_file = sqlContext.sql('SELECT * FROM ' + tbl_aud_mu_bs)
  df_aud_log_file_update_row = df_aud_log_file.filter((col('nombre_unidad_virtual') == nombre_unidad_virtual) & (col('contenedor') == contenedor))
  
  if tipo_modificacion == 'creacion':
    zone = pytz.timezone('America/Bogota')
    date = datetime.now(zone)
    current_date = date.strftime("%d/%m/%Y %H:%M:%S")

    if df_aud_log_file_update_row.count() == 0:
      newRow = spark.createDataFrame([(nombre_unidad_virtual, contenedor, current_date)])      
      df_aud_log_file = df_aud_log_file.unionAll(newRow)   
      df_aud_log_file.write.format('delta').mode('overwrite').option('path', path_aud_mu_bs).saveAsTable(tbl_aud_mu_bs)  
      
  elif tipo_modificacion == 'eliminacion':
    
    if df_aud_log_file_update_row.count() > 0:
      df_aud_log_file = df_aud_log_file.filter((col('nombre_unidad_virtual') != nombre_unidad_virtual) & (col('contenedor') != contenedor))
      df_aud_log_file.write.format('delta').mode('overwrite').option('path', path_aud_mu_bs).saveAsTable(tbl_aud_mu_bs)  
  else:
    pass    

# COMMAND ----------

# DBTITLE 1,Actualizar Tabla Log Aud Mapeo Unidades - Data Lake
def update_panel_auditoria_log_mapeo_unidades_DataLake(nombre_unidad_virtual, key_name, key_pwd):
  df_aud_log_file = sqlContext.sql('SELECT * FROM ' + tbl_aud_mu_dla)
  df_aud_log_file_update_row = df_aud_log_file.filter((col('nombre_unidad_virtual') == nombre_unidad_virtual) & 
                                                      (col('key_name') == key_name) & 
                                                      (col('key_pwd') == key_pwd))
  if tipo_modificacion == 'creacion':
    if df_aud_log_file_update_row.count() == 0:
          newRow = spark.createDataFrame([(nombre_unidad_virtual, key_name, key_pwd, current_date)])      
          df_aud_log_file = df_aud_log_file.unionAll(newRow)   
          df_aud_log_file.write.format('delta').mode('overwrite').option('path', path_aud_mu_dla).saveAsTable(tbl_aud_mu_dla)

  elif tipo_modificacion == 'eliminacion':

    if df_aud_log_file_update_row.count() > 0:
      df_aud_log_file = df_aud_log_file.filter((col('nombre_unidad_virtual') != nombre_unidad_virtual) & 
                                               (col('key_name') != key_name) & 
                                               (col('key_pwd') != key_pwd))
      df_aud_log_file.write.format('delta').mode('overwrite').option('path', path_aud_mu_dla).saveAsTable(tbl_aud_mu_dla)
      df_aud_log_file.show()

# COMMAND ----------

# DBTITLE 1,Desmontar Unidad Virtual - Data Lake
"""
Descripcion: Se encarga de eliminar las unidades virtuales que se envian mediante el parametro
             - Argument (dic_mount_points) y el parametro TipoModificacion = eliminacion
Autor: Juan Escobar E, Blaimir Ospina.
Fecha: 03/06/2020.
"""

def fn_unmount_data_lake(arg_dic_mount_points):
  
  msg_return = 'No se encontraron nuevas unidades para desmontar en el Data Lake'
  str_acum_mounted_units = 'Unidades desmontadas del Data Lake: '
  
  for point in dic_mount_points["applications_data_lake"]:
    if os.path.exists(FILE_SYSTEM + MOUNT_POINT_UNIT + point['nombre_unidad_virtual']):
      try:
        dbutils.fs.unmount(MOUNT_POINT_UNIT + point['nombre_unidad_virtual'])
        update_panel_auditoria_log_mapeo_unidades_DataLake(point['nombre_unidad_virtual'], 
                                                           point['key_name'], 
                                                           point['key_pwd'])
        
      except Exception:
        msg_return = sys.exc_info()       
  return msg_return

# COMMAND ----------

# DBTITLE 1,Desmontar Unidad Virtual - Blob Storage
"""
Descripcion: Se encarga de eliminar las unidades virtuales que se envian mediante el parametro
             - Argument (dic_mount_points) y el parametro TipoModificacion = eliminacion
Autor: Juan Escobar E, Blaimir Ospina.
Fecha: 03/06/2020.
"""

def fn_unmount_blob_storage(dic_mount_points):
  msg_return = 'No se encontraron nuevas unidades para demontar en el Storage Account'
  str_acum_mounted_units = 'Unidades desmontadas del Blob Storage: '
 
  for point in dic_mount_points['applications_storage']:
      try:
        print(FILE_SYSTEM + MOUNT_POINT_UNIT + point['nombre_unidad_virtual'])
        if os.path.exists(FILE_SYSTEM + MOUNT_POINT_UNIT + point['nombre_unidad_virtual']):
          
          dbutils.fs.unmount(MOUNT_POINT_UNIT + point['nombre_unidad_virtual'])
        
#           str_acum_mounted_units = str_acum_mounted_units + point['nombre_unidad_virtual'] + '\n'
#           msg_return = str_acum_mounted_units
          
          # Regitrar log en tabla Zdr_Auditoria_Log_Mapeo_Unidades_Blob_Storage
          update_panel_auditoria_log_mapeo_unidades_blob_storage(point['nombre_unidad_virtual'], point['contenedor'])
          
      except Exception:
          msg_return = sys.exc_info() 
          
  return msg_return    

# COMMAND ----------

# DBTITLE 1,Consultar Unidades Montadas
def print_units():
  list_unidades = dbutils.fs.ls('/mnt')
  display(list_unidades)

# COMMAND ----------

# DBTITLE 1,Ejecución Mapeo Unidades - Data Lake - Blob Storage
"""
Descripción: Esta función se encarga de la creación ó eliminanción de una unidad virtual entre
             Azure Databrciks - Azure Blob storage | Azure Databrciks - Azure Data Lake.
Autor: Juan David Escobar E, Balimir Ospina C.
Ejecucion: ejecucion_mapeo_unidades()
Fecha: 03/06/2020
"""

def ejecucion_mapeo_unidades():
  key_dic_arg = list(dic_mount_points.keys())[0]
  
  #.Crear las tablas de auditoria del DataLake y BlosStorage si no existe. 
  #.Monta la unidadd de rdh-auditoria en el datalake
  init_aud_mapeo_unidades()
  
  if tipo_modificacion == 'creacion':    
    if key_dic_arg == 'applications_storage':
      init_mapeo_unidades_blob_storage()
      fn_mapeo_unidades_storage_account(dic_mount_points)
      
    elif key_dic_arg == 'applications_data_lake':
      init_mapeo_unidades_data_lake()
      fn_mapeo_unidades_data_lake(dic_mount_points)
      
  elif tipo_modificacion == 'eliminacion':
    
    if key_dic_arg == 'applications_storage':
      init_mapeo_unidades_blob_storage()
      fn_unmount_blob_storage(dic_mount_points)
    elif key_dic_arg == 'applications_data_lake':
      init_mapeo_unidades_data_lake()
      fn_unmount_data_lake(dic_mount_points)     
  else:
    print_units()
