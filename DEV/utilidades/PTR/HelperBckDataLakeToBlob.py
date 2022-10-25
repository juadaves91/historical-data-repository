# Databricks notebook source
# DBTITLE 1,Import librerias
import shutil
from datetime import datetime
import pytz
import csv
from os import path, walk, makedirs
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,Variable global
"""
Descripcion: Inicializacion de variables globales.
Fecha: 18/06/2020
"""

fileName = 'bck_list_control_files_raw_error_dl.csv'
print('fileName: ', fileName)

# COMMAND ----------

# DBTITLE 1,Create file in Blob Storage
"""
Descripción: Verifica si el archivo CSV se encuentra creado, de lo contrario, lo crea en la ruta local del 
             contenedor y posteriormente lo mueve a la carpeta del bck-data-lake. El archivo se creara para almacenar 
             un inventario de los JSON que se encuentren en RAW o ERROR
Parametros:
            prm_path_src = '/dbfs/mnt/lotus-comex-storage/' (Ruta de creación)
            prm_path_sink = '/dbfs/mnt/lotus-comex-storage/bck-data-lake/' (Ruta destino)
            prm_list_headers = ['application', 'subapplication', 'file', 'folder', 'date'] (Headers del archivo)
Autor: Juan Escobar, Diego Velasco.
Fecha: 28/05/2020
"""
# path_src, path_sink, list_headers
def createFile(prm_dict_params):
  print('Paso 1 Debug..')
  exist = path.exists(str(prm_dict_params['path_sink'] + '/'))
  print('Existe Ruta destino: ', exist)
  
  if (not path.exists(str(prm_dict_params['path_sink'] + '/'))):
    makedirs(prm_dict_params['path_sink'])
    print(prm_dict_params['path_sink'])
    print('Paso 2')
  
  print('Paso 2.1')
  if (not path.exists(str(prm_dict_params['path_sink'] + '/' + fileName))):
    print('Paso 3')
    with open((prm_dict_params['path_src'] + '/' + fileName), 'a+', encoding='utf-8') as csv_file:
      file_writer = csv.writer(csv_file, delimiter='|', quotechar='"')
      file_writer.writerow(prm_dict_params['list_headers'])
    shutil.move((prm_dict_params['path_src'] + '/' + fileName), (prm_dict_params['path_sink'] + '/' + fileName))
    
print('CMD3 - HelperBckDataLakeToBlob')

# COMMAND ----------

# DBTITLE 1,Update control   file with name files
"""
Descripción: Actualiza el archivo de control con la aplicación, subaplicación, nombre del archivo, carpeta donde se encuentra (raw, error)
             y fecha de registro.
Parametros:
            prm_dict_params = {
              'application': 'comex',
              'subapplication': 'N/A',
              'path_root_db':'/dbfs',
              'path_sink': '/mnt/lotus-comex-storage/bck-data-lake/',
              'filter_col': 'file',
              'path_app_data_lake': '/dbfs/mnt/lotus-comex/'
            }
            prm_folder_file = 'raw', 'error'
Autor: Juan Escobar, Diego Velasco.
Fecha: 28/05/2020
"""

def updateControlFile(prm_dict_params, prm_folder_file):
  zone = pytz.timezone('America/Bogota')
  date = datetime.now(zone)
  current_date = date.strftime("%d/%m/%Y %H:%M:%S")
  current_date = datetime.strptime(current_date, "%d/%m/%Y %H:%M:%S")

  # recorre directorios y archivos de: /dbfs/mnt/lotus-comex/ raw o error
  for root, dirs, files in walk(prm_dict_params['path_app_data_lake'] + '/' + prm_folder_file):
    for file in files:
      # prm_dict_params['path_sink'] = /mnt/lotus-comex-storage/bck-data-lake/bck_list_control_files_raw_error_dl.csv
      df_files = spark.read.csv((prm_dict_params['path_sink'] + '/' + fileName), header=True, sep="|")
      df_files = df_files.filter(col(prm_dict_params['filter_col']) == file)
      if df_files.count() == 0:
        #prm_dict_params['path_root_db'] = '/dbfs/mnt/lotus-comex-storage/bck-data-lake/bck_list_control_files_raw_error_dl.csv'
        with open((prm_dict_params['path_root_db'] + prm_dict_params['path_sink'] + '/' + fileName), 'a+', encoding='utf-8') as csv_file:
          file_writer = csv.writer(csv_file, delimiter='|', quotechar='"')
          file_writer.writerow([
            prm_dict_params['application'],
            prm_dict_params['subapplication'],
            file,
            prm_folder_file,
            str(current_date)
          ])
        csv_file.close()
        
print('CMD4 - HelperBckDataLakeToBlob')
        
#""" Nota: Remplazar a+ por w+ en caso de que un json pase de estar en error y pase a raw este debe ser eliminado del archivo"""

# COMMAND ----------

"""
Descripción: copiado de la información almacenada en formato .parquet de la tablas de la app Comex en el Blob Storage
para respaldar la información en caso de desastres (Ver PTR de RDH).
Autor: Juan Escobar, Diego Velasco.
Fecha: 28/05/2020.
"""

def copyBck(prm_dict_params):
  #   Ejemplo:
  #   path_root = "/dbfs"
  #   path_folder_results_data_lk = "/mnt/lotus-comex/results"
  #   path_folder_results_blob = "/mnt/lotus-comex-storage/bck-data-lake/lotus-comex/results"

  if (path.exists(prm_dict_params['path_root'] + prm_dict_params['path_folder_results_blob'])):
    shutil.rmtree(prm_dict_params['path_root'] + prm_dict_params['path_folder_results_blob'], ignore_errors = True)
    print('Ruta: ' + prm_dict_params['path_folder_results_blob'] + ' eliminada exitosamente en el Blob Storage.')

  shutil.copytree(
    (prm_dict_params['path_root'] + prm_dict_params['path_folder_results_data_lk']),
    (prm_dict_params['path_root'] + prm_dict_params['path_folder_results_blob'])
  )
  print('Ruta: ' + prm_dict_params['path_folder_results_blob'] + ' creada exitosamente en el Blob Storage.')
  
print('CMD5 - HelperBckDataLakeToBlob')
