# Databricks notebook source
#Validaciones: https://support.ptc.com/help/servigistics/insadmin_hc/es/index.html#page/Servigistics_InService_Administration_Help_Center/InS_UtilValidatingCSVData.html

#1. Encoding. -- validate_encoding_csv(ar_file) - Validación de Encoding - UTF-8
#2. Md5. -- validate_md5_csv(ar_md5, ar_file) - Validar integridad CSV (MD5)
#3. Filas vacias. -- validate_row_empty(ar_file) - Validación de lineas vacias
#4. Duplicados. -- fn_get_duplicates(ar_df, ar_records_unique) - Validación de duplicados
#5. Fechas. -- validates_format_date_colums(ar_columns, ar_file) - Validar columnas tipo fecha

# COMMAND ----------

# DBTITLE 1,Import Libs
import pandas as pd
import os
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
import chardet
from datetime import datetime
import hashlib
import csv

# COMMAND ----------

# DBTITLE 1,Variables globales
this_encoding = 'UTF-8'

# COMMAND ----------

# DBTITLE 1,Validación de Encoding - UTF-8
"""
Descripción: Retorna boolean que determina si el archivo cuenta con el encoding UTF-8.
Responsables: Juan David Escobar E, Diego Alexander Velasco.
Fecha: 06/04/2020
"""

def validate_encoding_csv(ar_file):
  ar_file = "/dbfs" + ar_file
  result = chardet.detect(open(ar_file, 'rb').read())
  charenc = result['encoding']
  return  (True, '') if this_encoding in charenc.upper() else (False, 'Encoding ' + charenc + ' invalido, debe ser ' + this_encoding)                    

# COMMAND ----------

# DBTITLE 1,Validar integridad CSV (MD5)
"""
Descripción: Retorna boolean que determina si el archivo que se cargo esta completo en el DataLake, se compara 
             MD5 que llega por parametro al Notebook, el cual se leyo previamente del archivo de popiedades_app_CSV 
             con un Hash MD5 del archivo leido en el notebook actual.
Responsables: Juan David Escobar E, Diego Alexander Velasco.
Fecha: 03/04/2020
"""
#NOTA: Borrar ruta de ar_file
def validate_md5_csv(ar_md5, ar_file):
  # Rutas
  ar_file = "/dbfs" + ar_file
  md5_db = hashlib.md5(open(ar_file, 'rb').read()).hexdigest()
  return (True, '') if md5_db == ar_md5 else (False, 'MD5 generado ' + md5_db + ' no concuerda con el enviado: ' + ar_md5)

# COMMAND ----------

# DBTITLE 1,Validación de lineas vacias
"""
Descripción: Retorna boolean que determina si el archivo no contienen ninguna línea vacía en el centro del fichero, al realizar la lectura la ultima line no importa si esta se encuentra vacia, si son varias lineas vacias al final, ignora la ultima y toma las anteriores.
Responsables: Juan David Escobar E, Diego Alexander Velasco.
Fecha: 06/04/2020
"""

def validate_row_empty(ar_file):
  empty = False
  isRowEmpty = ''
  line = 1
  lines_empty = 0
  separator = ''
  lines_fails = ''
  result = ''
  ar_file = "/dbfs" + ar_file
  try:
    with open(ar_file, 'r', encoding = this_encoding) as csvfile:
      csvreader = csv.reader(csvfile, delimiter='|', quotechar='"')
      next(csvreader)
      for row in csvreader:
        lenRow = len(row)
        if row in (None, "") or lenRow == 0:
          lines_empty += 1
          lines_fails += separator + str(line)
          separator = ','
        line += 1
    if lines_empty > 0:
      empty = True
      isRowEmpty = 'Las filas (' + str(lines_fails) + ') del archivo se encuentra vacia.'
  except Exception as error:
    empty = True
    isRowEmpty = 'No fue posible analizar las lineas vacias. !ERROR¡: ' + str(error)
  return (not(empty), isRowEmpty)

# COMMAND ----------

# DBTITLE 1,Validación de duplicados
"""
Descripción: Retorna los registros duplicados a partir de un Dataframe, y los registros unicos
Parámetros:
    ar_file -- Archivo a validar
    gb_records -- String el cual contiene los nombres de la columna que son unicos del Dataframe.
Responsables: Juan David Escobar E, Diego Alexander Velasco.
Fecha: 03/04/2020
"""
# ar_records_unique = "IdRegistro"
def fn_get_duplicates(ar_file, ar_records_unique):
  is_error = False
  msg_error = ''
  result = ''
  separator = ''
  try:
    df_csv = spark.read.csv(ar_file, header=True, sep="|")
  
    df_Campo = df_csv.groupby(ar_records_unique).count()
    df_duplicados = df_Campo.select(col(ar_records_unique), col("count")).filter(col("count") > 1).collect()
    duplicados = [str(ar_records_unique + ": " + row[ar_records_unique] + " - Cantidad: " + str(row['count'])) for row in df_duplicados]
    
    for i in range(len(duplicados)):
      lista_duplicados = duplicados[i].split(",")
      msg_error += separator + "["+(lista_duplicados[0].replace('"',''))+"]"
      separator = ', '
    if len(duplicados) == 0:
      is_error = True
      msg_error = ''
    result = (is_error, msg_error)
  except Exception as error:
    is_error = False
    msg_error = 'No se pudo validar duplicados. !ERROR¡: ' + str(error)
    result = (is_error, msg_error)
  return result

# COMMAND ----------

# DBTITLE 1,Validación de formato fecha
"""
Descripción: Valida el formato de los valores de campos tipo str_date que estan almacenados en el 
             archivo CSV insumo, esta función recibe 2 parametros y retorna un valor booleano dependiendo 
             si la fecha es valida o invalida, tambien retorna un mensaje de error o vacio dependiendo de 
             el exito o fracaso de la validación, los parametros se extraen del diccionario de datos de 
             la app, los campos (NombreCampo, Formato).
Parámetros:
    ar_date_text -- String de la fecha a validar.
    ar_format    -- String el cual contiene el valor del formato de fecha a validar.
Return: 
    (isValidFormat, valueError) -- Tuple.
Responsables: Juan David Escobar E, Diego Alexander Velasco.
Fecha: 04/04/2020.

Ejecucion: print(validate_format_date('2003-12-23'))
           print(validate_format_date('10/02/1991'))
"""

def validate_format_date(ar_column_name, ar_date_text, ar_format = "%d/%m/%Y"):
  isValidFormat = True
  valueError = ""
  try:
    datetime.strptime(ar_date_text, ar_format)
  except ValueError:
    isValidFormat = False
    valueError = "la columna: '" + ar_column_name  + "', tiene un formato invalido, debe ser: '" + ar_format + "'"
  finally:
    return isValidFormat, valueError

# COMMAND ----------

# DBTITLE 1,Validar columnas tipo fecha
"""
Descripción: Recorre cada columna de tipo fecha realizando la validación segun el formato estipulado
Parámetros:
    ar_file -- String ruta del archivo.
    ar_columns -- Array de diccionario con las llaves 'column_name' y 'format_column'. 
                  EJ: [{'column_name':'FechaActualizacion', 'format_column':'%Y-%m-%d %H:%M:%S.%m'}, {'column_name':'dtmFecha','format_column':'%Y-%m-%d %H:%M:%S'}]
Responsables: Juan David Escobar E, Diego Alexander Velasco.
Fecha: 07/04/2020.
"""
# ar_columns = [{'column_name':'FechaActualizacion', 'format_column':'%Y-%m-%d %H:%M:%S.%m'}, {'column_name':'dtmFecha','format_column':'%Y-%m-%d %H:%M:%S'}]
# ar_file = "/mnt/dbbicorporativo-indicadores/raw/dbbicorporativo_20200318_001.csv"
def validates_format_date_colums(ar_columns, ar_file):
  df_csv = spark.read.csv(ar_file, header=True, sep="|")

  errors = ''
  separator = ''
  is_error = 0

  
  if ar_columns[0]['column_name'] is not '':
    for column in ar_columns:
      colum_name = column["column_name"]
      format_column = column["format_column"]
      index = 1
      df_csv_aux = df_csv.select(col(colum_name))
      iterate = df_csv_aux.rdd.map(lambda x: (x[colum_name]))
      for iteration in iterate.collect():
        ar_date_text = iteration
        index += 1
        is_validate_format_date = validate_format_date(colum_name, ar_date_text, format_column)
        if not(is_validate_format_date[0]):
          is_error += 1
          errors += separator + "En la fila '" + str(index) + "' " + is_validate_format_date[1]
          separator = '\n'
          
    return (False, errors) if is_error > 0 else (True, '')
  
  else:
    return (False, errors) if is_error > 0 else (True, '')

# COMMAND ----------

"""
Descripción: En caso de que no pase por lo menos una de las validaciones
             el archivo csv se mueve el archivo CSV a la carpeta error, en caso de que
             pase la validaciones va y elemina el archivo CSV en error en caso 
             de que exista para asi no dupplicar los archivos.
Parámetros:
    ar_error -- Diccionario con los errores de la validacion del CSV
    ar_path  -- Ruta del archivo CSV

Responsable: Blaimir Ospina Cardona.
Fecha: 11/08/2020.

"""

def validations_error(ar_error, ar_path):
  if ar_error[0] == '' and ar_error[1] == '' and ar_error[2] == '' and ar_error[3] == '' and ar_error[4] == '':
    dbutils.fs.rm(ar_path.replace('raw', 'error'))
  else:
    dbutils.fs.mv(ar_path, ar_path.replace('raw', 'error'))

# COMMAND ----------

# DBTITLE 1,Inicializador de validaciones
"""
Descripción: Llama los metodos de validación del CSV y retorna los mensajes de error.
Parámetros:
    ar_dic_prm_init -- Diccionario con parametros {ar_md5: String, ar_file: String, ar_date_text: String, ar_format: String}.
    ar_format    -- String el cual contiene el valor del formato de fecha a validar.
Return: 
    msg -- Array con mensajes de error.
Responsables: Juan David Escobar E, Diego Alexander Velasco.
Fecha: 07/04/2020.

msg[0] - Validación de Encoding - UTF-8
msg[1] - Validar integridad CSV (MD5)
msg[2] - Validación de lineas vacias
msg[3] - Validación de duplicados
msg[4] - Validar columnas tipo fecha
"""
def init_validations(ar_dic_prm_init):
  
  
  msg = []
  ar_file = ar_dic_prm_init['file']
  ar_md5 = ar_dic_prm_init['md5']
  ar_records_unique = ar_dic_prm_init['records_unique']
  ar_columns = ar_dic_prm_init['columns_date']
  
  is_validate_encoding_csv = validate_encoding_csv(ar_file)
  is_validate_md5_csv = validate_md5_csv(ar_md5, ar_file)
  is_validate_row_empty = validate_row_empty(ar_file)
  is_validates_format_date_colums = validates_format_date_colums(ar_columns, ar_file)
  
  # si el parametro que trae ar_records_unique es vacio no se debe validar la existencia de duplicacos de los contrario  entonces si
  if ar_records_unique == '':
    is_fn_get_duplicates = (True, '')
  else:
    is_fn_get_duplicates = fn_get_duplicates(ar_file, ar_records_unique)
  
  if not(is_validate_encoding_csv[0]):
    msg.append(is_validate_encoding_csv[1])
  else:
    msg.append('')
    
  if not(is_validate_md5_csv[0]):
    msg.append(is_validate_md5_csv[1])
  else:
    msg.append('')
    
  if not(is_validate_row_empty[0]):
    msg.append(is_validate_row_empty[1])
  else:
    msg.append('')
    
  if not(is_fn_get_duplicates[0]):
    msg.append(is_fn_get_duplicates[1])
  else:
    msg.append('')
    
  if not(is_validates_format_date_colums[0]):
    msg.append(is_validates_format_date_colums[1])
  else:
    msg.append('')
  
  validations_error(msg, ar_dic_prm_init['file'])
  
  return msg
