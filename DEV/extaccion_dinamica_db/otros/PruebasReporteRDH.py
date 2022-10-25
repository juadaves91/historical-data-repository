# Databricks notebook source
myArray = [1,2,1,3,3,1,2,1,5,1]
myArray.sort()
min_val =  min(myArray)
max_val =  max(myArray)
array_num = range(min_val, max_val + 1)


for i, val in enumerate(range(min_val, max_val + 1)):
  conteo = myArray.count(array_num[i]) * '*' 
  print(val, ':', conteo)
  
  
  #print('{0}: {2}'.format(val, conteo))
 

  

    

# COMMAND ----------

fatten = l: [item for sublist in l for item in sublist] print(flatten([[1],[2],[3],[4,5]])) # Output: [1, 2, 3, 4, 5] 
flatten = lambda l: [item for sublist in l for item] print(flatten([[1],[2],[3],[4,5]])) # Output: [1, 2, 3, 4, 5] 
flatten = lambda l: [item for sublist in l for item in sublist] print(flatten([[1],[2],[3],[4,5]])) # Output: [1, 2, 3, 4, 5] 
flatten = lambda l: [item for sublist in l for item in sublist] print(flat([[1],[2],[3],[4,5]])) # Output: [1, 2, 3, 4, 5] 
	

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM AUDITORIA.zdr_auditoria_log_extraction_bd_tbl_in_blob;

# COMMAND ----------



# COMMAND ----------

import os

#Variavles Globales Staticas
file_location = "/mnt/app-igconbank/"
mount = '/dbfs'
zona_crudos = '/raw/'
zona_result = '/results/'
name_df = 'panel_'
data_base = 'igconbank'
name_table = 'Zdr_'

# Diccionario Dataframe Dinamicos
panels_frames = {}

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = "|"
file_type = "csv"

# COMMAND ----------

"""
Descripción: Crea la Base de Datos en caso de que no exista.
Responsable: Blaimir Ospina Cardona.
Fecha: 08/25/2020
"""


def ft_create_dataBase():
  try:
     sqlContext.sql('create database if not exists ' + data_base)
  except Exception:
    print('Error al crear la Base de datos')

# COMMAND ----------

"""
Descripción: Crea los dataframe Dinammicos.
Responsable: Blaimir Ospina Cardona.
Fecha: 08/25/2020
"""

def ft_create_df(ar_path, ar_app):
  try:
    panels_frames[name_df + ar_app] = spark.read.format(file_type) \
                    .option("inferSchema", infer_schema) \
                    .option("header", first_row_is_header) \
                    .option("sep", delimiter) \
                    .load(ar_path)
    print('Dataframe:', ar_app,  'Creado')
  except Exception:
    return Exception

# COMMAND ----------

"""
Descripción: Recorre cada una de las rutas
Responsable: Blaimir Ospina Cardona.
Fecha: 08/25/2020
"""

def ft_recorrer_folders():
  for app in os.listdir(mount + file_location):
    try:
      ft_create_df((file_location + app + zona_crudos), app)
    except Exception as error:
      print('Error al crear el dataframe: ' + app)
      print(Exception)

# COMMAND ----------

"""
Descripción: Materializar y Almacenar las tablas en la zona de resultados
Responsable: Blaimir Ospina Cardona.
Fecha: 08/25/2020
"""

def ft_materializar_tablas():
  try:
    for dataframe in panels_frames.keys():
        sqlContext.sql('drop table if exists ' + data_base + "." + name_table +  data_base + '_' + dataframe)
        panels_frames[dataframe].repartition(1
                                            ).write.format('delta'
                                            ).mode("overwrite"
                                            ).option('path', file_location + dataframe[dataframe.index('_') + 1:] + zona_result + dataframe
                                            ).saveAsTable(data_base + "." + name_table +  data_base + '_' + dataframe)
        
        print('Tabla: ' + data_base + "." + name_table +  data_base + '_' + dataframe + ' Creada correctamente')
   
  except Exception as error:
    print('Error al almacenar la informacion o la tabla:' , dataframe)
    print(error)

# COMMAND ----------

def ft_init():
  try:
    ft_create_dataBase()
    ft_recorrer_folders()
    ft_materializar_tablas()
  except Exception:
    print(Exception)

ft_init()

# COMMAND ----------

df = spark.read.csv('/mnt/app-igconbank/cruceinternolibro/raw', header = True, sep = ',')

# COMMAND ----------

class unpivot:
  
#   __id_principal = 'ID'
  
  def __tamaño_columnas(self, ar_df):
    try:
      tamano_columnas = len(ar_df.columns) - 1
      return tamano_columnas
    
    except Exception as error:
      print("Error alnas consulta el numero de columnas \n" + error)
  
  def __construccion_query(self, ar_df):
    try:
      query = 'stack(' + str(self.__tamaño_columnas(ar_df))

      for columnas in ar_df.columns:
        if columnas == ar_df.columns[0]:
          pass
        else:
          query = query + ",'" + columnas + "'," + columnas
      query = query + ') as (key , value)'
      return query
    
    except Exception as error:
      print("Error al construir la sentencia \n" + error)
  #   
  def view_unpivot(self, ar_df):
    try:
      df_pivot = ar_df.selectExpr(ar_df.columns[0], self.__construccion_query(ar_df))
      return df_pivot
    
    except Exception as error:
      print("Error al Pivotear \n" + error)
  
objeto = unpivot()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl_prueba
# MAGIC where NUM_DOCU = 4643061

# COMMAND ----------

df = sqlContext.sql('select * from app_igconbank.zdr_app_igconbank_panel_extractobancario')

# COMMAND ----------

df.withColumn('id_estado_extractobancario', 'extractobancario')

# COMMAND ----------

sql = "select "
scapear = "'"
for columnas in df_extractobancario.columns:
  sql = sql + 'replace(' + columnas + ', ' + scapear + '"' + scapear + ', ' + scapear + scapear + ') as ' + columnas + ','

sql = sql + ' from ' + name_table
sql = sql.replace(', from', ' from')
df_temp = sqlContext.sql(sql)
sqlContext.registerDataFrameAsTable(df_temp, name_table_temp)
display(df_temp)

# COMMAND ----------

sql_1 = "select "
scapear = '"'
for columnas in df_extractobancario.columns:
  sql_1 = sql_1 + 'concat(' +  scapear + "'" + scapear + ', '+ columnas + ', ' +  scapear + "'" + scapear + ') as ' + columnas + ' ,'


sql_1 = sql_1 + ' from ' + name_table_temp
sql_1 = sql_1.replace(', from', ' from')
df_temp = sqlContext.sql(sql_1)
display(df_temp)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

df_index = df_temp.select("*").withColumn("id", monotonically_increasing_id())
display(df_index)

# COMMAND ----------

df_temp.repartition(1).write.mode('overwrite').csv('/mnt/app-igconbank/depuracion/extractobancario_depurado', sep = '|' , header = 'true')

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls /dbfs/mnt/app-semih-storage/SCHSEMIH/HISACUMANOCONCEPTO

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC # LECTURA TABLAS - JUAN DAVID ESCOBAR E 

# COMMAND ----------

# DBTITLE 1,1. Lectura de tablas en DF
"""
df_HISACUMANOCONCEPT 98.942.940 registros, fecha = NOMFECHA
df_HISDATOSEMPLEO = 32533 registros, fecha = PSRFECHA
df_HISFUEROSINDICAL = 959 registros, fecha = PSRFECHA
"""

df_HISACUMANOCONCEPT = spark.read.format('parquet').load('/mnt/app-semih/SCHSEMIH/raw/HISACUMANOCONCEPT')
df_HISDATOSEMPLEO = spark.read.format('parquet').load('/mnt/app-semih-storage/SCHSEMIH/HISDATOSEMPLEO')
df_HISFUEROSINDICAL = spark.read.format('parquet').load('/mnt/app-semih-storage/SCHSEMIH/HISFUEROSINDICAL')

# COMMAND ----------

# DBTITLE 1,2.1 Creación de columna calculada tipo fecha - formato 'dd/MM/yyyy'
"""
1. Substring de fecha en formato 'dd/MM/yyyy HH:MM:SS' convertida en 'dd/MM/yyyy'.
2. Casteo del Substring 'dd/MM/yyyy' a tipo fecha en formato 'dd/MM/yy'.
3. Formateo de fecha 'dd/MM/yy' a un formato 'dd/MM/yyyy' DATE.
"""

from pyspark.sql.functions import substring, col, lit, to_date, date_format

# df_result_cast_HISACUMANOCONCEPT =  df_HISACUMANOCONCEPT.withColumn('FECHA_BUSQUEDA', date_format(to_date(substring(col('NOMFECHA'), 1, 8), 'dd/MM/yy'), 'dd/MM/yyyy'))
df_result_cast_HISACUMANOCONCEPT = df_HISACUMANOCONCEPT
df_result_cast_HISDATOSEMPLEO =  df_HISDATOSEMPLEO.withColumn('FECHA_BUSQUEDA', date_format(to_date(substring(col('PSRFECHA'), 1, 8), 'dd/MM/yy'), 'dd/MM/yyyy'))
df_result_cast_HISFUEROSINDICAL =  df_HISFUEROSINDICAL.withColumn('FECHA_BUSQUEDA', date_format(to_date(substring(col('PSRFECHA'), 1, 8), 'dd/MM/yy'), 'dd/MM/yyyy'))

# COMMAND ----------

# DBTITLE 1,2 Casteo dataframe a String
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql.functions import col

df_result_cast_HISACUMANOCONCEPT = df_result_cast_HISACUMANOCONCEPT.withColumn('NOMNUMID', col("NOMNUMID").cast('string'))\
                                                                      .withColumn('IDENTIFICADOR', col("IDENTIFICADOR").cast('string'))\
                                                                      .withColumn('NOMFECHA', col("NOMFECHA").cast('string'))\
                                                                      .withColumn('NOMA', col("NOMA").cast('string'))\
                                                                      .withColumn('NOMCCOSTOS', col("NOMCCOSTOS").cast('string'))\
                                                                      .withColumn('NOMTDOC', col("NOMTDOC").cast('string'))\
                                                                      .withColumn('NOMCONCEPT', col("NOMCONCEPT").cast('string'))\
                                                                      .withColumn('NOMVALOR', col("NOMVALOR").cast('string'))\
                                                                      .withColumn('NOMHORA', col("NOMHORA").cast('string'))\
                                                                      .withColumn('NOMANO', col("NOMANO").cast('string'))\
                                                                      .withColumn('NOMPERIODO', col("NOMPERIODO").cast('string'))\
                                                                      .withColumn('ANIO_1',substring(col('FECHA_BUSQUEDA'), 7,4))\
                                                                      .withColumn('MES_1',substring(col('FECHA_BUSQUEDA'), 4,2))\
                                                                      .withColumn('DIA_1', substring(col('FECHA_BUSQUEDA'), 1,2))\
                                                                      .withColumn("NOMBRE_TABLA",lit("HISACUMANOCONCEPT"))

df_result_cast_HISDATOSEMPLEO = df_result_cast_HISDATOSEMPLEO.withColumn('PSRNUMID', col("PSRNUMID").cast('string'))\
                                                              .withColumn('PSRNOMB', col("PSRNOMB").cast('string'))\
                                                              .withColumn('PSRFECCON', col("PSRFECCON").cast('string'))\
                                                              .withColumn('PSRFECREC', col("PSRFECREC").cast('string'))\
                                                              .withColumn('PSRFECESE', col("PSRFECESE").cast('string'))\
                                                              .withColumn('PSRFECANT', col("PSRFECANT").cast('string'))\
                                                              .withColumn('PSRFECREI', col("PSRFECREI").cast('string'))\
                                                              .withColumn('PSRFECSER', col("PSRFECSER").cast('string'))\
                                                              .withColumn('PSREXPROF', col("PSREXPROF").cast('string'))\
                                                              .withColumn('PSRULTVER', col("PSRULTVER").cast('string'))\
                                                              .withColumn('PSRULTINC', col("PSRULTINC").cast('string'))\
                                                              .withColumn('PSRFECPRU', col("PSRFECPRU").cast('string'))\
                                                              .withColumn('PSRNUMBEN', col("PSRNUMBEN").cast('string'))\
                                                              .withColumn('PSRORIDES', col("PSRORIDES").cast('string'))\
                                                              .withColumn('PSRACTIVI', col("PSRACTIVI").cast('string'))\
                                                              .withColumn('PSRTELTRA', col("PSRTELTRA").cast('string'))\
                                                              .withColumn('PSRDEPPOS', col("PSRDEPPOS").cast('string'))\
                                                              .withColumn('PSRDESCDEP', col("PSRDESCDEP").cast('string'))\
                                                              .withColumn('PSRFECHA', col("PSRFECHA").cast('string'))\
                                                              .withColumn('IDENTIFICADOR', col("IDENTIFICADOR").cast('string'))\
                                                              .withColumn('ANIO',substring(col('PSRFECHA'), 7,4))\
                                                              .withColumn('MES',substring(col('PSRFECHA'), 4,2))\
                                                              .withColumn('DIA', substring(col('PSRFECHA'), 1,2))\
                                                              .withColumn("NOMBRE_TABLA",lit("HISDATOSEMPLEO"))

df_result_cast_HISFUEROSINDICAL = df_result_cast_HISFUEROSINDICAL.withColumn('PSRNUMID', col("PSRNUMID").cast('string'))\
                                                                  .withColumn('PSRNOMB', col("PSRNOMB").cast('string'))\
                                                                  .withColumn('PSRMANDATO', col("PSRMANDATO").cast('string'))\
                                                                  .withColumn('PSRFECINI', col("PSRFECINI").cast('string'))\
                                                                  .withColumn('PSRFECFIN', col("PSRFECFIN").cast('string'))\
                                                                  .withColumn('PSRSINDICA', col("PSRSINDICA").cast('string'))\
                                                                  .withColumn('PSRCIUDAD', col("PSRCIUDAD").cast('string'))\
                                                                  .withColumn('PSRFECASA', col("PSRFECASA").cast('string'))\
                                                                  .withColumn('PSRFECARTA', col("PSRFECARTA").cast('string'))\
                                                                  .withColumn('PSRFECRES', col("PSRFECRES").cast('string'))\
                                                                  .withColumn('PSRRESOL', col("PSRRESOL").cast('string'))\
                                                                  .withColumn('PSRFECHA', col("PSRFECHA").cast('string'))\
                                                                  .withColumn('IDENTIFICADOR', col("IDENTIFICADOR").cast('string'))\
                                                                  .withColumn('ANIO',substring(col('PSRFECHA'), 7,4))\
                                                                  .withColumn('MES',substring(col('PSRFECHA'), 4,2))\
                                                                  .withColumn('DIA', substring(col('PSRFECHA'), 1,2))\
                                                                  .withColumn("NOMBRE_TABLA",lit("HISFUEROSINDICAL"))

# COMMAND ----------

# DBTITLE 1,2.2 Creación de columna calculada FECH_BUSQUEDA_INT
from pyspark.sql.functions import substring, col, lit, to_date, date_format, concat

df_result_cast_HISACUMANOCONCEPT =  df_result_cast_HISACUMANOCONCEPT.withColumn('FECHA_BUSQUEDA_INT', concat(col("ANIO_1"), col("MES_1"), col("DIA_1")).cast('int'))
df_result_cast_HISDATOSEMPLEO =  df_result_cast_HISDATOSEMPLEO.withColumn('FECHA_BUSQUEDA_INT', concat(col("ANIO"), col("MES"), col("DIA")).cast('int'))
df_result_cast_HISFUEROSINDICAL =  df_result_cast_HISFUEROSINDICAL.withColumn('FECHA_BUSQUEDA_INT', concat(col("ANIO"), col("MES"), col("DIA")).cast('int'))

# COMMAND ----------

df_result_cast_HISACUMANOCONCEPT.printSchema()
# df_result_cast_HISDATOSEMPLEO.printSchema()
# df_result_cast_HISFUEROSINDICAL.printSchema()

# COMMAND ----------

#display(df_result_cast_HISACUMANOCONCEPT)
display(df_result_cast_HISDATOSEMPLEO)
#display(df_result_cast_HISFUEROSINDICAL)

# COMMAND ----------

from pyspark.sql.functions import substring, col, lit, to_date, date_format, concat


df = df_result_cast_HISACUMANOCONCEPT.where("FECHA_BUSQUEDA_INT = '20141111'")
display(df)

# COMMAND ----------

# DBTITLE 1,Borrado de tablas
# MAGIC %sh
# MAGIC rm -rf /dbfs/mnt/app-semih/SCHSEMIH/results/panel_hisacumanoconcept
# MAGIC rm -rf /dbfs/mnt/app-semih/SCHSEMIH/results/panel_hisdatosempleo
# MAGIC rm -rf /dbfs/mnt/app-semih/SCHSEMIH/results/panel_hisfuerosindical

# COMMAND ----------

# DBTITLE 1,3. Materializar las tablas
df_result_cast_HISACUMANOCONCEPT.repartition('ANIO','MES'
                                            ).write.format('Delta'
                                            ).mode('overwrite'
                                            ).partitionBy('ANIO','MES'
                                            ).save('/mnt/app-semih/SCHSEMIH/results/panel_hisacumanoconcept')
          

df_result_cast_HISDATOSEMPLEO.repartition('ANIO','MES'
                                          ).write.format('Delta'
                                          ).mode('overwrite'
                                          ).partitionBy('ANIO','MES'
                                          ).save('/mnt/app-semih/SCHSEMIH/results/panel_hisdatosempleo')



df_result_cast_HISFUEROSINDICAL.repartition('ANIO','MES'
                                            ).write.format('Delta'
                                            ).mode('overwrite'
                                            ).partitionBy('ANIO','MES'
                                            ).save('/mnt/app-semih/SCHSEMIH/results/panel_hisfuerosindical')

# COMMAND ----------

display(df_result_cast_HISFUEROSINDICAL)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS SEMIH.Zdr_panel_hisacumanoconcept;
# MAGIC CREATE TABLE SEMIH.Zdr_panel_hisacumanoconcept
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/app-semih/SCHSEMIH/results/panel_hisacumanoconcept';
# MAGIC 
# MAGIC DROP TABLE IF EXISTS SEMIH.Zdr_panel_hisdatosempleo;
# MAGIC CREATE TABLE SEMIH.Zdr_panel_hisdatosempleo
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/app-semih/SCHSEMIH/results/panel_hisdatosempleo';
# MAGIC 
# MAGIC DROP TABLE IF EXISTS SEMIH.Zdr_panel_hisfuerosindical;
# MAGIC CREATE TABLE SEMIH.Zdr_panel_hisfuerosindical
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/app-semih/SCHSEMIH/results/panel_hisfuerosindical';

# COMMAND ----------

# MAGIC %md
# MAGIC # LECTURA TABLAS - JUAN DAVID ESCOBAR E - FINAL

# COMMAND ----------

# df_HISACUMANOCONCEPT = '/mnt/app-semih-storage/SCHSEMIH/HISACUMANOCONCEPTO'
# df_HISDATOSEMPLEO = '/mnt/app-semih-storage/SCHSEMIH/HISACUMANOCONCEPTO' 
# df_HISFUEROSINDICAL = '/mnt/app-semih-storage/SCHSEMIH/HISFUEROSINDICAL'

# COMMAND ----------

# MAGIC %sh 
# MAGIC 
# MAGIC ls /dbfs/mnt/app-semih/SCHSEMIH/raw/HISACUMANOCONCEPT

# COMMAND ----------

df_HISACUMANOCONCEPT = spark.read.format('parquet').load('/mnt/app-semih-storage/SCHSEMIH/HISACUMANOCONCEPTO')
df_HISDATOSEMPLEO = spark.read.format('parquet').load('/mnt/app-semih-storage/SCHSEMIH/HISDATOSEMPLEO')
df_HISFUEROSINDICAL = spark.read.format('parquet').load('/mnt/app-semih-storage/SCHSEMIH/HISFUEROSINDICAL')

# COMMAND ----------

from pyspark.sql.functions import col, substring, lit

# COMMAND ----------

"""
1. Generar una tabla de 100 millones de registros HISACUMANOCONCEPT formato Delta
2. Crear tabla unpivot apartir de las tablas:
   HISACUMANOCONCEPT
   HISDATOSEMPLEO
   HISFUEROSINDICAL
   en formato Delta, particiones:
                     Nombre tabla
                     annio
                     mes
"""

# COMMAND ----------

df_HISACUMANOCONCEPT = spark.read.load('/mnt/app-semih/SCHSEMIH/raw/HISACUMANOCONCEPT')

# COMMAND ----------

df_string_1 = df_HISACUMANOCONCEPT.select(col("NOMNUMID"), col("IDENTIFICADOR"),col("NOMFECHA"), col("NOMA"), col("NOMTURNO"), col("NOMCCOSTOS"), col("NOMTDOC"), col("NOMCONCEPT"), col("NOMNOMB"), col("NOMVALOR").cast("string"), col("NOMHORA").cast("string"), col('NOMANO').cast("string"), col("NOMPERIODO").cast("string"), substring(col('NOMFECHA'), 7,4).alias("ANIO"), "MES")

# COMMAND ----------

for index in range(1,13,2):
  df = df_string_1.withColumn('MES', lit(index))
  df.repartition(1).write.format('parquet').mode('append').save('/mnt/app-semih/SCHSEMIH/raw/HISACUMANOCONCEPT')

# COMMAND ----------

import pyspark.sql.functions as F

# df_stadicticas = spark.read.load('/mnt/app-semih/SCHSEMIH/raw/HISACUMANOCONCEPT')

# df_f = df_stadicticas.groupBy('MES').count().sort('MES')
# df_f.show()

# COMMAND ----------

df_f.agg(F.sum('count').alias('Contidad de IRegistros')).show()

# COMMAND ----------

"""
Crea los 100 millones de registros 
"""

for index in range(0,20):
  df_HISACUMANOCONCEPT.repartition(1).write.format('parquet').mode('append').save('/mnt/app-semih/SCHSEMIH/raw/HISACUMANOCONCEPT')

# COMMAND ----------

"""Alamcenar"""

df_HISACUMANOCONCEPT = spark.read.load('/mnt/app-semih/SCHSEMIH/raw/HISACUMANOCONCEPT')

# COMMAND ----------

"""
castear dataframe a string
"""
df_string_1 = df_HISACUMANOCONCEPT.select(col("NOMNUMID"), col("IDENTIFICADOR"),col("NOMFECHA"), col("NOMA"), col("NOMTURNO"), col("NOMCCOSTOS"), col("NOMTDOC"), col("NOMCONCEPT"), col("NOMNOMB"), col("NOMVALOR").cast("string"), col("NOMHORA").cast("string"), col('NOMANO').cast("string"), col("NOMPERIODO").cast("string"), substring(col('NOMFECHA'), 7,4).alias("ANIO"),col("MES").cast("string").alias("MES"))


df_string_2 = df_HISDATOSEMPLEO.select('PSRNUMID',
                                       'PSRNOMB', 
                                       'PSRFECCON', 
                                       'PSRFECREC', 
                                       'PSRFECESE', 
                                       'PSRFECANT', 
                                       'PSRFECREI', 
                                       'PSRFECSER', 
                                       'PSREXPROF', 
                                       'PSRULTVER', 
                                       'PSRULTINC', 
                                       'PSRFECPRU', 
                                       col('PSRNUMBEN').cast('string').alias('PSRNUMBEN'), 
                                       'PSRORIDES', 
                                       'PSRACTIVI', 
                                       'PSRTELTRA', 
                                       'PSRDEPPOS', 
                                       'PSRDESCDEP', 
                                       'PSRFECHA', 
                                       'IDENTIFICADOR',
                                       substring(col('PSRFECHA'), 7,4).alias("ANIO"),
                                       substring(col('PSRFECHA'), 4,2).alias('MES')
                                      )

df_string_3 = df_HISFUEROSINDICAL.select("*",  substring(col('PSRFECHA'), 7,4).alias("ANIO"), substring(col('PSRFECHA'), 4,2).alias('MES'))

# COMMAND ----------

"""
castear dataframe a string
"""
df_string_1 = df_HISACUMANOCONCEPT.select(col("NOMNUMID"), col("IDENTIFICADOR"),col("NOMFECHA"), col("NOMA"), col("NOMTURNO"), col("NOMCCOSTOS"), col("NOMTDOC"), col("NOMCONCEPT"), col("NOMNOMB"), col("NOMVALOR").cast("string"), col("NOMHORA").cast("string"), col('NOMANO').cast("string"), col("NOMPERIODO").cast("string"), substring(col('NOMFECHA'), 7,4).alias("ANIO"),col("MES").cast("string").alias("MES"))


df_string_2 = df_HISDATOSEMPLEO.select('PSRNUMID',
                                       'PSRNOMB', 
                                       'PSRFECCON', 
                                       'PSRFECREC', 
                                       'PSRFECESE', 
                                       'PSRFECANT', 
                                       'PSRFECREI', 
                                       'PSRFECSER', 
                                       'PSREXPROF', 
                                       'PSRULTVER', 
                                       'PSRULTINC', 
                                       'PSRFECPRU', 
                                       col('PSRNUMBEN').cast('string').alias('PSRNUMBEN'), 
                                       'PSRORIDES', 
                                       'PSRACTIVI', 
                                       'PSRTELTRA', 
                                       'PSRDEPPOS', 
                                       'PSRDESCDEP', 
                                       'PSRFECHA', 
                                       'IDENTIFICADOR',
                                       substring(col('PSRFECHA'), 7,4).alias("ANIO"),
                                       substring(col('PSRFECHA'), 4,2).alias('MES')
                                      )

df_string_3 = df_HISFUEROSINDICAL.select("*",  substring(col('PSRFECHA'), 7,4).alias("ANIO"), substring(col('PSRFECHA'), 4,2).alias('MES'))

# COMMAND ----------

df_string_1 = spark.read.format('Delta').load('/mnt/app-semih/SCHSEMIH/results/panel_hisacumanoconcept') 
df_string_2 = spark.read.format('Delta').load('/mnt/app-semih/SCHSEMIH/results/panel_hisdatosempleo') 
df_string_3 = spark.read.format('Delta').load('/mnt/app-semih/SCHSEMIH/results/panel_hisfuerosindical')

# COMMAND ----------

df_string_1 = df_string_1.withColumn('TABLA', lit('hisacumanoconcept'))
df_string_2 = df_string_2.withColumn('TABLA', lit('hisdatosempleo'))
df_string_3 = df_string_3.withColumn('TABLA', lit('hisfuerosindical'))

# COMMAND ----------

from pyspark.sql.functions import substring
df_string_3 = df_string_3.select('PSRNUMID',
                                 'PSRNOMB',
                                 'PSRMANDATO',
                                 'PSRFECINI',
                                 'PSRFECFIN',
                                 'PSRSINDICA',
                                 'PSRCIUDAD',
                                 'PSRFECASA',
                                 'PSRFECARTA',
                                 'PSRFECRES',
                                 'PSRRESOL',
                                 substring(col('PSRFECHA'), 1, 8).cast('date'),
                                 'IDENTIFICADOR',
                                 'ANIO',
                                 'MES',
                                 'TABLA')

# COMMAND ----------

display(df_string_1)

# COMMAND ----------

from pyspark.sql.functions import col, substring, to_date,date_format

# date_format(col('FECHA_FILTRO_1'),"dd/MM/yyyy")

df_test = df_HISACUMANOCONCEPT.select(date_format(to_date(substring(col('NOMFECHA'), 1,10),'dd/MM/yyyy'),'dd/MM/yyyy').alias('FECHA_FILTRO'))

# COMMAND ----------

df_test.printSchema()

# COMMAND ----------


df_string_1 = df_HISACUMANOCONCEPT.select('*', substring(col("NOMFECHA"),1,10).alias('FECHA_FILTRO'), substring(col('NOMFECHA'), 7,4).alias("ANIO"), substring(col('NOMFECHA'), 4,2).alias('MES'))
df_string_2 = df_HISDATOSEMPLEO.select('*', substring(col("PSRFECHA"),1,10).alias('FECHA_FILTRO'), substring(col('PSRFECHA'), 7,4).alias("ANIO"), substring(col('PSRFECHA'), 4,2).alias('MES'))
df_string_3 =  df_HISFUEROSINDICAL.select('*', substring(col("PSRFECHA"),1,10).alias('FECHA_FILTRO'), substring(col('PSRFECHA'), 7,4).alias("ANIO"), substring(col('PSRFECHA'), 4,2).alias('MES'))

# df_string_1 = df_string_1.select('*', col('FECHA_FILTRO').cast('date')) 
# df_string_2 = df_string_2.select('*', col('FECHA_FILTRO').cast('date')) 
# df_string_3 = df_string_3.select('*', col('FECHA_FILTRO').cast('date')) 

# df_string_1 = df_string_1.withColumn('TABLA', lit('panel_hisacumanoconcept'))
# df_string_2 = df_string_2.withColumn('TABLA', lit('panel_hisdatosempleo'))
# df_string_3 = df_string_3.withColumn('TABLA', lit('panel_hisfuerosindical'))

# COMMAND ----------

df_string_1.printSchema()

# COMMAND ----------

from pyspark.sql.functions import concat

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /dbfs/mnt/app-semih/SCHSEMIH/results/panel_hisacumanoconcept_1
# MAGIC rm -rf /dbfs/mnt/app-semih/SCHSEMIH/results/panel_hisdatosempleo_1
# MAGIC rm -rf /dbfs/mnt/app-semih/SCHSEMIH/results/panel_hisfuerosindical_1

# COMMAND ----------

df_string_1.repartition('ANIO','MES'
                        ).write.format('Delta'
                        ).mode('overwrite'
                        ).partitionBy('ANIO','MES'
                        ).save('/mnt/app-semih/SCHSEMIH/results/panel_hisacumanoconcept_1')
          

df_string_2.repartition('ANIO','MES'
                        ).write.format('Delta'
                        ).mode('overwrite'
                        ).partitionBy('ANIO','MES'
                        ).save('/mnt/app-semih/SCHSEMIH/results/panel_hisdatosempleo_1')



df_string_3.repartition('ANIO','MES'
                        ).write.format('Delta'
                        ).mode('overwrite'
                        ).partitionBy('ANIO','MES'
                        ).save('/mnt/app-semih/SCHSEMIH/results/panel_hisfuerosindical_1')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS SEMIH.Zdr_panel_hisacumanoconcept;
# MAGIC CREATE TABLE SEMIH.Zdr_panel_hisacumanoconcept
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/app-semih/SCHSEMIH/results/panel_hisacumanoconcept';
# MAGIC 
# MAGIC DROP TABLE IF EXISTS SEMIH.Zdr_panel_hisdatosempleo;
# MAGIC CREATE TABLE SEMIH.Zdr_panel_hisdatosempleo
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/app-semih/SCHSEMIH/results/panel_hisdatosempleo';
# MAGIC 
# MAGIC DROP TABLE IF EXISTS SEMIH.Zdr_panel_hisfuerosindical;
# MAGIC CREATE TABLE SEMIH.Zdr_panel_hisfuerosindical
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/app-semih/SCHSEMIH/results/panel_hisfuerosindical';

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE SEMIH.Zdr_panel_hisacumanoconcept ZORDER BY (NOMNUMID);
# MAGIC OPTIMIZE SEMIH.Zdr_panel_hisacumanoconcept ZORDER BY (NOMNOMB)

# COMMAND ----------

var = 'HISACUMANOCONCEPT'
print(var.lower())

# COMMAND ----------

display(df_string_1.select("mes").distinct())

# COMMAND ----------

df_unpivot_1 = df_string_1.selectExpr("NOMNUMID", "MES", "ANIO", "stack(12,'IDENTIFICADOR',IDENTIFICADOR,'NOMFECHA',NOMFECHA,'NOMA',NOMA,'NOMTURNO',NOMTURNO,'NOMCCOSTOS',NOMCCOSTOS,'NOMTDOC',NOMTDOC,'NOMCONCEPT',NOMCONCEPT,'NOMNOMB',NOMNOMB,'NOMVALOR',NOMVALOR,'NOMHORA',NOMHORA,'NOMANO',NOMANO,'NOMPERIODO',NOMPERIODO,'ANIO') as (KEY , VALUE)").withColumn("TABLA",lit("HISACUMANOCONCEPT"))


df_unpivot_2 = df_string_2.selectExpr(df_string_2.columns[0] ,"MES", "ANIO", "stack(19,'PSRNOMB',PSRNOMB,'PSRFECCON',PSRFECCON,'PSRFECREC',PSRFECREC,'PSRFECESE',PSRFECESE,'PSRFECANT',PSRFECANT,'PSRFECREI',PSRFECREI,'PSRFECSER',PSRFECSER,'PSREXPROF',PSREXPROF,'PSRULTVER',PSRULTVER,'PSRULTINC',PSRULTINC,'PSRFECPRU',PSRFECPRU,'PSRNUMBEN',PSRNUMBEN,'PSRORIDES',PSRORIDES,'PSRACTIVI',PSRACTIVI,'PSRTELTRA',PSRTELTRA,'PSRDEPPOS',PSRDEPPOS,'PSRDESCDEP',PSRDESCDEP,'PSRFECHA',PSRFECHA,'IDENTIFICADOR',IDENTIFICADOR) as (KEY , VALUE)").withColumn("TABLA",lit("HISDATOSEMPLEO"))


df_unpivot_3 = df_string_3. selectExpr( df_string_3.columns[0],"MES", "ANIO","stack(12,'PSRNOMB',PSRNOMB,'PSRMANDATO',PSRMANDATO,'PSRFECINI',PSRFECINI,'PSRFECFIN',PSRFECFIN,'PSRSINDICA',PSRSINDICA,'PSRCIUDAD',PSRCIUDAD,'PSRFECASA',PSRFECASA,'PSRFECARTA',PSRFECARTA,'PSRFECRES',PSRFECRES,'PSRRESOL',PSRRESOL,'PSRFECHA',PSRFECHA,'IDENTIFICADOR',IDENTIFICADOR) as (KEY , VALUE)").withColumn("TABLA",lit("HISFUEROSINDICAL"))

# COMMAND ----------

print(df_unpivot_1.count())
print(df_unpivot_2.count())
print(df_unpivot_3.count())

# COMMAND ----------

append = df_unpivot_1.unionAll(df_unpivot_2)
append = append.unionAll(df_unpivot_3)

# COMMAND ----------

append.select("mes").distinct().show()

# COMMAND ----------

append.repartition('TABLA','ANIO','MES').write.format('Delta').mode('overwrite').partitionBy('TABLA','ANIO','MES').save('/mnt/app-semih/SCHSEMIH/results/panel_unpivot_peoplesoft_semih')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS SEMIH.Zdr_SemiH_Prueba;
# MAGIC CREATE TABLE SEMIH.Zdr_SemiH_Prueba
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/app-semih/SCHSEMIH/results/panel_unpivot_peoplesoft_semih';

# COMMAND ----------

display(df_HISACUMANOCONCEPT)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SEMIH.Zdr_SemiH_Prueba
# MAGIC where TABLA = 'HISACUMANOCONCEPT'

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE SEMIH.Zdr_SemiH_Prueba ZORDER BY (NOMNUMID)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), tabla from SEMIH.Zdr_SemiH_Prueba
# MAGIC group by tabla

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table semih.filtros;
# MAGIC create table semih.filtros
# MAGIC (
# MAGIC MES INT,
# MAGIC ANIO string,
# MAGIC TABLA string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE semih.filtros VALUES (11, '2014', 'HISFUEROSINDICAL');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (11, '2014', 'HISDATOSEMPLEO');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (1, '2014', 'HISACUMANOCONCEPT');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (2, '2014', 'HISACUMANOCONCEPT');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (3, '2014', 'HISACUMANOCONCEPT');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (4, '2014', 'HISACUMANOCONCEPT');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (5, '2014', 'HISACUMANOCONCEPT');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (6, '2014', 'HISACUMANOCONCEPT');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (7, '2014', 'HISACUMANOCONCEPT');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (8, '2014', 'HISACUMANOCONCEPT');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (9, '2014', 'HISACUMANOCONCEPT');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (10, '2014', 'HISACUMANOCONCEPT');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (11, '2014', 'HISACUMANOCONCEPT');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (12, '2014', 'HISACUMANOCONCEPT');

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE semih.filtros VALUES (11, '2014', 'hisfuerosindical');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (11, '2014', 'hisdatosempleo');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (1, '2014', 'hisacumanoconcept');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (2, '2014', 'hisacumanoconcept');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (3, '2014', 'hisacumanoconcept');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (4, '2014', 'hisacumanoconcept');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (5, '2014', 'hisacumanoconcept');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (6, '2014', 'hisacumanoconcept');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (7, '2014', 'hisacumanoconcept');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (8, '2014', 'hisacumanoconcept');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (9, '2014', 'hisacumanoconcept');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (10, '2014', 'hisacumanoconcept');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (11, '2014', 'hisacumanoconcept');
# MAGIC INSERT INTO TABLE semih.filtros VALUES (12, '2014', 'hisacumanoconcept');

# COMMAND ----------

# MAGIC %sql