# Databricks notebook source
# DBTITLE 1,Importación de librerias
import os
import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime
import pytz
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

# DBTITLE 1,Mapeo de variables
"""Path Ruta de la Aplicacion"""
dbutils.widgets.text("application", "","")
dbutils.widgets.get("application")
application = getArgument("application")

Path = '/dbfs/mnt/lotus-' + application + '/'
PathParquet = '/mnt/rdh-auditoria/results/lotus_auditoria_anexos_json'
permanent_table_name = "auditoria.Zdr_lotus_auditoria_anexos_json"

# COMMAND ----------

#Metodo para segmentar niveles de lectura para las subcarpetas de una ruta
def walklevel(some_dir, level=1):
    some_dir = some_dir.rstrip(os.path.sep)
    assert os.path.isdir(some_dir)
    num_sep = some_dir.count(os.path.sep)
    for root, dirs, files in os.walk(some_dir):
        yield root, dirs, files
        num_sep_this = root.count(os.path.sep)
        if num_sep + level <= num_sep_this:
            del dirs[:]

# COMMAND ----------

# DBTITLE 1,Creación Schema
schema_source_aux = StructType([
  StructField("src_application", StringType(), True),
  StructField("src_application_sub", StringType(), True),
  StructField("src_name_zip", StringType(), True),
  StructField("src_length", StringType(), True)
])

dummy = ['{"UniversalID":"string","UniversalIDCompuesto":"string","attachments":["array"]}']

sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

# DBTITLE 1,Obtener anexos
def getAttachments(sub_folder):
  ar_path = '/mnt/rdh-auditoria/auxiliares/' + application + '.txt'
  df_zip = spark.read.csv(ar_path,header=True, sep="|")
  df_zip = df_zip.filter(col('application') == sub_folder)
  
  return df_zip

# COMMAND ----------

def createAuditJson(df_auditoria):
  df_auditoria.write.format("parquet").mode("append").save(PathParquet)
  df_auditoria_temp = spark.read.parquet(PathParquet)
  df_auditoria = df_auditoria_temp.drop_duplicates(['application', 'zips_json', 'zips_blob', 'missing_zips'])
  df_auditoria.write.format("parquet").mode("overwrite").save(PathParquet)

# COMMAND ----------

# DBTITLE 1,Comparación de JSON vs ZIPS
def compare(df_js, sub_folder):
  zone = pytz.timezone('America/Bogota')
  date = datetime.now(zone)
  current_date = date.strftime("%d/%m/%Y %H:%M:%S")

  df_aud_zip_pandas = pd.DataFrame(data=None, columns = ['application', 'zips_json', 'zips_blob', 'missing_zips', 'date'])
  df_zip = getAttachments(sub_folder)
  df_unid_json = df_js.filter(col('attachments') > 0)
  
  df_zip_json = df_unid_json.alias('L').join(df_zip.alias('D'), col('L.UniversalID') == col('D.name'), how='fullouter').select(col('L.application'), col('D.name'), col('L.UniversalID')).filter(col('D.name').isNotNull())
  
  df_missing_zips = df_unid_json.alias('L').join(df_zip.alias('D'), col('L.UniversalID') == col('D.name'), how='fullouter').select(col('L.UniversalID'), col('L.file')).filter(col('D.name').isNull()).collect()
  
  missing_zips = [str(row['UniversalID'] + ' - ' + row['file']) for row in df_missing_zips]
  missing_list = ''
  for i in range(len(missing_zips)):
    Universalid = missing_zips[i].split(",")
    missing_list += "["+(Universalid[0].replace('"',''))+"] "
  
  application_sub = application
  
  if application == 'reclamos' or application == 'pedidos':
    application_sub = application_sub + "-" + sub_folder
  
  current_date = datetime.strptime(current_date, "%d/%m/%Y %H:%M:%S")
  new_row = {'application':application_sub, 'zips_json':df_unid_json.count(), 'zips_blob':df_zip_json.count(), 'missing_zips':missing_list, 'date': current_date}
  
  df_aud_zip_pandas = df_aud_zip_pandas.append(new_row,ignore_index=True)
  df_aud_zip = sqlContext.createDataFrame(df_aud_zip_pandas)
  createAuditJson(df_aud_zip)

# COMMAND ----------

def missing_zip_in_json(df_js, sub_folder):
  df_zip = getAttachments(sub_folder)
  df_unid_json = df_js.filter(col('attachments') > 0)
  
  df_missing_in_json_aux = df_unid_json.alias('L').join(df_zip.alias('D'), col('L.UniversalID') == col('D.name'), how='fullouter').select(col('D.application').alias('src_application'),col('D.name').alias('src_name_zip'), col('D.length').alias('src_length')).filter(col('L.UniversalID').isNull()).collect()
  
  missing_in_json = [str("'src_application':'" + application + "','src_application_sub'" + ": '" + row['src_application'] + "', 'src_name_zip': '" + row['src_name_zip'] + "', 'src_length': '" + row['src_length'] + "'") for row in df_missing_in_json_aux]
  arr_rows = []
  newRow = {}
  for i in range(len(missing_in_json)):
    missing_in_json_aux = str("{" + missing_in_json[i] + "}")
    newRow = eval(missing_in_json_aux)
    arr_rows.append(newRow)
  return arr_rows

# COMMAND ----------

# DBTITLE 1,Lectura de JSON
level = 1

if application == 'reclamos' or application == 'pedidos':
  level = 2

df_aux_pandas = pd.DataFrame(data = None, columns = ['src_application', 'src_application_sub', 'src_name_zip', 'src_length'])
df_json_pandas_aux = pd.DataFrame(data = None, columns = ['UniversalID', 'attachments', 'file', 'application'])

sub_folder = ""
sub_folder_aux = ""
arr_rows = []
arr_sub_folder = []
for root, dirs, files in walklevel(Path, level):
  for file in files:
    if file.startswith('lotus_') & file.endswith(".json"):
      js = root + '/' + file
      js = js[5:]
      df_json = sqlContext.read.schema(schema_json.schema).json(js)
      sqlContext.registerDataFrameAsTable(df_json, "tbl_df_json")
      root_temp = root.split('/')
      #Verificación de subaplicación (EJ: pedidos-subpedidos)
      sub_folder_aux = ""
      if len(root_temp) == 6:
        sub_folder = root_temp[len(root_temp)-2]
        sub_folder_aux += "-" + sub_folder
      else:
        sub_folder = application
      arr_sub_folder.append(str(sub_folder))
        
      lines_df = df_json.count()
      if lines_df > 0:
        df_json = sqlContext.sql('SELECT IF(UniversalIDCompuesto IS NULL,UniversalID, UniversalIDCompuesto) AS UniversalID, attachments FROM tbl_df_json')
        df_json = df_json.select(col('UniversalID'), col('attachments'))
        df_json = df_json.select("UniversalID", array_join("attachments", '\r\n ').alias("attachments"))
        df_json = df_json.select(col('UniversalID'), length(col('attachments')).cast("integer").alias('attachments'))
        df_json_pandas = df_json.toPandas()
        df_json_pandas['file'] = file
        df_json_pandas['application'] = sub_folder
        
        df_json_pandas_aux = df_json_pandas_aux.append(df_json_pandas)
        df_js = sqlContext.createDataFrame(df_json_pandas_aux)

for application_sub in arr_sub_folder:
  arr_rows = []
  df_js_new = df_js.filter(col("application") == application_sub)
  compare(df_js_new, application_sub)
  
  rows = missing_zip_in_json(df_js_new, application_sub)
  arr_rows.extend(rows)
  if len(arr_rows) > 0:
    df_aux_pandas = df_aux_pandas.append(arr_rows,ignore_index=True)

df_source = sqlContext.createDataFrame(df_aux_pandas, schema_source_aux)

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC La tabla Zdr_lotus_auditoria_anexos conta de 5 columnas (application, zips_json, zips_blob, missing_zips, date)
# MAGIC 
# MAGIC application: Contiene el nombre de la aplicación ejecutada y de la subaplicación en caso de pedidos o reclamos (EJ: pedidos-subpedidos, comex)
# MAGIC zips_json: Cantidad de lineas que cuentan con el campo Attachment diferente de cero, es decir, Attachments:[]
# MAGIC zips_blob: Cantidad de archivos .zip que estan en el Blob Storage para cada aplicación.
# MAGIC missing_zips: UniversalID con anexos que no se encuentraron dentro del Blob Storage
# MAGIC date: Fecha de ejecución
# MAGIC */
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS auditoria;
# MAGIC USE auditoria;
# MAGIC DROP TABLE IF EXISTS Zdr_lotus_auditoria_anexos_json;
# MAGIC 
# MAGIC CREATE TABLE Zdr_lotus_auditoria_anexos_json 
# MAGIC USING PARQUET
# MAGIC LOCATION '/mnt/rdh-auditoria/results/lotus_auditoria_anexos_json';

# COMMAND ----------

# MAGIC %run /auditoria/Auditoria_Anexos_Zips
