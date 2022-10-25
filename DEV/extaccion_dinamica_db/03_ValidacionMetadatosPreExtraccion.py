# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC # VALIDACION METADATOS USUARIO EXTRACCION Y CREACION DE TABLAS METADATOS

# COMMAND ----------

# DBTITLE 1,Importación de librerías
import pandas as pd
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
import json
from pyspark.sql.types import StructField,IntegerType, StructType,StringType

# COMMAND ----------

# DBTITLE 1,EJEMPLO EJECUCIÓN SCRIPT
"""
EJEMPLO DE EJECUCIÓN:

PARÁMETROS:

1. COUNT_ROWS_TBL_ORIGEN = '"[{"COUNT(*)":32533.0}]"'
2. ESQUEMA_TBL_ORIGEN = 
"[{"TABLE_NAME":"HISDATOSEMPLEO","COLUMN_NAME":"IDENTIFICADOR","DATA_TYPE":"VARCHAR2","DATA_TYPE_EXT":"VARCHAR2(14)","CHAR_LENGTH":14.0,"DATA_LENGTH":56.0,"DATA_PRECISION":null,"DATA_SCALE":null,"NULLABLE":"N","DATA_DEFAULT":null},{"TABLE_NAME":"HISDATOSEMPLEO","COLUMN_NAME":"PSRNOMB","DATA_TYPE":"VARCHAR2","DATA_TYPE_EXT":"VARCHAR2(75)","CHAR_LENGTH":75.0,"DATA_LENGTH":300.0,"DATA_PRECISION":null,"DATA_SCALE":null,"NULLABLE":"Y","DATA_DEFAULT":null},{"TABLE_NAME":"HISDATOSEMPLEO","COLUMN_NAME":"PSRNUMID","DATA_TYPE":"VARCHAR2","DATA_TYPE_EXT":"VARCHAR2(15)","CHAR_LENGTH":15.0,"DATA_LENGTH":60.0,"DATA_PRECISION":null,"DATA_SCALE":null,"NULLABLE":"Y","DATA_DEFAULT":null}]
"

3. NOMBRE_TBL_METADATOS_USUARIO  = SEMIH.ZDR_SEMIH_SRC_METADATOS
4. NOMBRE_TBL_ORIGEN = APP-SEMIH/SCHSEMIH/HISDATOSEMPLEO
5. TBL_INVENTARIOS_TBLS_BD = SEMIH.ZDR_SEMIH_SRC_METADATOS_INVENTARIO_TABLAS_BD
6. TIPO_BD  = ORACLE
"""

# COMMAND ----------

# DBTITLE 1,Mapeo de variables
"""Path Ruta de la Aplicacion"""

# Tabla actual a extraer de la BS
dbutils.widgets.text("nombre_tbl_origen", "","")
dbutils.widgets.get("nombre_tbl_origen")
nombre_tbl_origen = getArgument("nombre_tbl_origen")
nombre_tbl_origen = nombre_tbl_origen.upper().strip()
sub_start = nombre_tbl_origen.rfind('/') + 1
sub_end = len(nombre_tbl_origen)
nombre_tbl_origen = nombre_tbl_origen[sub_start:sub_end ]  if '/' in nombre_tbl_origen else nombre_tbl_origen

# Esquema extraido de la BD origen
dbutils.widgets.text("esquema_tbl_origen", "","")
dbutils.widgets.get("esquema_tbl_origen")
esquema_tbl_origen = getArgument("esquema_tbl_origen")

# Tabla utilizada para consultar la metadata asociada a la tabla actual, esta tabla de metadatos es diligenciada en el archivo Formato_ArchivosFuente_Generico.xlsx
dbutils.widgets.text("nombre_tbl_metadatos_usuario", "","")
dbutils.widgets.get("nombre_tbl_metadatos_usuario")
nombre_tbl_metadatos_usuario = getArgument("nombre_tbl_metadatos_usuario")
nombre_tbl_metadatos_usuario = nombre_tbl_metadatos_usuario.upper().strip()

# Tipo de Bd
dbutils.widgets.text("tipo_bd", "","")
dbutils.widgets.get("tipo_bd")
tipo_bd = getArgument("tipo_bd")
tipo_bd = tipo_bd.upper().strip()

# Boolean para establecer si los metadatos diligenciados por el usuario en el archivo Formato_ArchivosFuente_Generico.xlsx coinciden con el de la BD (VALIACIONES: Metadatos, cantidad columnas esquema, cantidad de filas tabla a extraer).
is_current_tbl_metadata_valid = True

# Conteo de filas de la tabla actual en la BD
count_rows_tbl_origen = getArgument("count_rows_tbl_origen")    
list_count_rows_tbl_origen = count_rows_tbl_origen[1 : len(count_rows_tbl_origen) - 1]
json_list_count_rows_tbl_origen = json.loads(list_count_rows_tbl_origen)
count_rows_tbl_origen = int(float(json_list_count_rows_tbl_origen[0]['COUNT(*)']))

# Conteo de filas de la tabla actual en la BD
dbutils.widgets.text("tbl_inventarios_tbls_bd", "","")
dbutils.widgets.get("tbl_inventarios_tbls_bd")
tbl_inventarios_tbls_bd = getArgument("tbl_inventarios_tbls_bd")
tbl_inventarios_tbls_bd = tbl_inventarios_tbls_bd.upper().strip()

list_prms = [{'nombre_tbl_origen':nombre_tbl_origen},
             {'esquema_tbl_origen':esquema_tbl_origen},
             {'nombre_tbl_metadatos_usuario':nombre_tbl_metadatos_usuario},
             {'tipo_bd':tipo_bd},
             {'is_current_tbl_metadata_valid':is_current_tbl_metadata_valid},
             {'count_rows_tbl_origen':count_rows_tbl_origen},
             {'tbl_inventarios_tbls_bd':tbl_inventarios_tbls_bd}]

print(list_prms)

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC # OBTENER DATAFRAMES TABLAS INSUMO VALIDACIÓN METADATOS

# COMMAND ----------

"""
-Descripción: Retorna un dataframe con la tabla de metadatos extraida la BD
-Nombre: Juan David Escobar E.
-Fecha: 24/08/2020
"""
def create_table_src_schema(prm_list_esquema_tbl_origen):
    
  # 1. init cols df
  keys = list(prm_list_esquema_tbl_origen[0].keys())
     
  # 2. Create pandas DF
  df_pd_list_esquema_tbl_origen = pd.DataFrame(prm_list_esquema_tbl_origen, columns = keys) 
  
  # 3. Create pyspark DF
  df_pys_list_esquema_tbl_origen = sqlContext.createDataFrame(df_pd_list_esquema_tbl_origen.astype(str))
  df_pys_list_esquema_tbl_origen.createOrReplaceTempView("df_tbl_list_esquema_tbl_origen")
  
  # 4. Casteo tipos de datos, importante para comparar los valores en metodos posteriores que hacen uso del dataframe
  query = "SELECT TABLE_NAME, " \
                 "CAST(DATA_PRECISION AS INT), " \
                 "COLUMN_NAME, " \
                 "CAST(CHAR_LENGTH AS INT), " \
                 "NULLABLE, DATA_TYPE, " \
                 "CAST(DATA_LENGTH AS INT), " \
                 "CAST(DATA_SCALE AS INT), " \
                 "DATA_DEFAULT " \
  "FROM df_tbl_list_esquema_tbl_origen"
    
  df_pys_list_esquema_tbl_origen = sqlContext.sql(query)  
  
  return df_pys_list_esquema_tbl_origen  

# COMMAND ----------

"""
-Descripción: Retorna un valor dataframe con la tabla de Inventario de Tablas de BD, extraida del archivo Formato_ArchivosFuente_Generico.xlsx
 -Nombre: Juan David Escobar E.
-Fecha: 24/08/2020
"""

def get_table_src_inventory_tables():
  
  # 1. Construcción de query para extraer los metadatos de la tabla a cargar, casteo de tipos de datos tipo enteros a comparar con la metadata del usuario.
  # 1.1.Casteo tipos de datos, importante para comparar los valores en metodos posteriores que hacen uso del dataframe    
  query = "SELECT NombreTablaAplicacion, " \
                 "CAST(CantidadRegistros AS INT) as CantidadRegistros " \
           "FROM " + tbl_inventarios_tbls_bd + " WHERE UPPER(RTRIM(LTRIM(NombreTablaAplicacion))) = UPPER(RTRIM(LTRIM(" + '"' + nombre_tbl_origen + '"' + ")))"
      
  sqlContext.sql('REFRESH TABLE ' + tbl_inventarios_tbls_bd)
  df_tbl_inventario_tablas_usuario = sqlContext.sql(query)  
    
  return df_tbl_inventario_tablas_usuario

# COMMAND ----------

"""
-Descripción: Retorna un valor dataframe con la tabla de metadatos extraida del archivo Formato_ArchivosFuente_Generico.xlsx
 -Nombre: Juan David Escobar E.
-Fecha: 28/08/2020
"""

def get_table_src_schema():
  
  # 1. Construcción de query para extraer la metadata de la tabla a cargar, casteo de tipos de datos tipo enteros a comparar con la metadata del usuario.
  # 1.1.Casteo tipos de datos, importante para comparar los valores en metodos posteriores que hacen uso del dataframe    
  query = "SELECT No, " \
                "OrigenAplicacion, " \
                "OrigenSubAplicacion, " \
                "Esquema, " \
                "TablaOrigen, " \
                "TablaDestino, " \
                "NombreCampoTecnico, " \
                "NombreCampoFuncional, " \
                "TipoDato, " \
                "Formato, " \
                "ValorNulo, " \
                "CAST(Longitud AS INT) as Longitud, " \
                "CAST(Precision AS INT) as Precision, " \
                "CAST(Escala AS INT) as Escala, " \
                "LlavePrimaria, " \
                "EsMigrable, " \
                "EsVisible, " \
                "EsCampoBusqueda, " \
                "FechaBusqueda " \
       "FROM " + nombre_tbl_metadatos_usuario + " WHERE UPPER(RTRIM(LTRIM(TablaOrigen))) = UPPER(RTRIM(LTRIM(" + '"' + nombre_tbl_origen + '"' + ")))"
  
  sqlContext.sql('REFRESH TABLE '+ nombre_tbl_metadatos_usuario)
  df_tbl_metadatos_usuario = sqlContext.sql(query)  
      
  return df_tbl_metadatos_usuario

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC # FUINCIONES DE VALIDACION

# COMMAND ----------

"""
-Descripción: Función que compara la cantidad filas registradas para la tabla actual en archivo Formato_ArchivosFuente_Generico.xlsx y 
 la cantidad de filas encontrada en la BD origen SELECT COUNT(*) FROM CurrentExtractTable.
-Nombre: Juan David Escobar E.
-Fecha: 28/08/2020
"""

def is_valid_metadata_count_rows_current_tbl(df_pys_tbl_metadatos_inventory_tables_usuario): 
  
  count_rows_inventory_tables = str(df_pys_tbl_metadatos_inventory_tables_usuario.select(F.col('CantidadRegistros')).collect()[0]['CantidadRegistros']).strip() 
  count_rows_inventory_tables = int(count_rows_inventory_tables)
  
#   if nombre_tbl_origen == 'HISDATOSEMPLEO':
#     count_rows_inventory_tables = 99999
  
  dic_result = {'is_valid': True, 'msg_error': ''} if count_rows_tbl_origen == count_rows_inventory_tables  else {'is_valid': False, 'msg_error': 'La cantidad de registros de la tabla: ' + nombre_tbl_origen + ' no coinciden, CantidadRegistros Formato_ArchivosFuente_Generico.xlsm: ' + str(count_rows_inventory_tables) + ', cantidad registros hallados en la BD: ' +  str(count_rows_tbl_origen)}
  
  print(dic_result)
  
  return dic_result

# COMMAND ----------

"""
-Descripción: Función que compara la cantidad de metadatos 'COLUMNAS' diligenciados en el archivo Formato_ArchivosFuente_Generico.xlsx y la BD.
-Nombre: Juan David Escobar E.
-Fecha: 28/08/2020
"""

def is_valid_metadata_count_rows(df_pys_list_esquema_tbl_origen, df_tbl_metadatos_usuario):
  count_meta_esquema_origen = df_pys_list_esquema_tbl_origen.count()
  count_meta_usr = df_tbl_metadatos_usuario.count()
  msg_error = '* Error: Comparando Metadatos BD - cantidad de columnas de metadatos no coinciden \n'
  msg_error = msg_error + '* Tabla: ' + nombre_tbl_origen + ' \n'
  dic_result = {}
  
  if count_meta_esquema_origen == count_meta_usr:
    dic_result = {'is_valid': True, 'msg_error': ''} 
  else:
    msg_error = msg_error + '* Descripcion: cantidad de columnas del esquema de la BD y Formato_ArchivoFuente_Generico no coinciden: ' + str(count_meta_esquema_origen) + ', ' + str(count_meta_usr) + ' \n'
    msg_error = msg_error + '------------------------------------------------------------------------------------------\n'
    print(msg_error)
    dic_result =  {'is_valid': False, 'msg_error': msg_error}
  
  return dic_result    

# COMMAND ----------

"""
-Descripción: Retorna un valor Boolean y un Str-MsgError para establecer si los metadatos 
 diligenciados por el usuario en el archivo Formato_ArchivosFuente_Generico.xlsx coincide con el de la BD.
-Nombre: Juan David Escobar E.
-Fecha: 24/08/2020
-Parámetros: @df_pys_list_esquema_tbl_origen: Tabla generada a partir de los metadatos
                                              de la tabla actual iterada desde la BD origen.
             @df_tbl_metadatos_usuario: Tabla generada a partir de la tabla actual, la 
                                        información proviene del usuario diligenciada 
                                        en el archivo Formato_ArchivoFuente_Generico.xlsx.
"""

def is_valid_metadata(df_pys_list_esquema_tbl_origen, df_tbl_metadatos_usuario):
      
  isValid = True
  
  msg_error = '* Error: Comparando Metadatos BD - Formato_ArchivoFuente_Generico.xlsx: \n'
  msg_error = msg_error + '* Tabla: ' + nombre_tbl_origen + ' \n' 
  
  for rowMetaUser in df_tbl_metadatos_usuario.collect():  
    
    # 1. Formateo de datos tomados de Excel diligenciado por usuario, eliminación de espacios en blanco, conversion de valores a mayusculas etc.
    
    nombre_campo_tecnico = str(rowMetaUser['NombreCampoTecnico']).upper().strip()
    tipo_dato = str(rowMetaUser['TipoDato']).upper().strip()
    tipo_dato_longitud = int(str(rowMetaUser['Longitud']).strip()) if str(rowMetaUser['Longitud']) != 'nan' else str(rowMetaUser['Longitud'])
    tipo_dato_precision = int(str(rowMetaUser['Precision']).strip()) if str(rowMetaUser['Precision']) != 'nan' else str(rowMetaUser['Precision'])
    tipo_dato_escala = int(str(rowMetaUser['Escala']).strip()) if str(rowMetaUser['Escala']) != 'nan' else str(rowMetaUser['Escala'])
    valor_nulo = str(rowMetaUser['ValorNulo']).replace("Si", "Y").replace("No", "N").upper().strip()
    
    # 2. Comparativo de metadatos BD vs Excel diligenciado por usuario Formato_ArchivosFuente_Generico.xlsx
    
    df_query_validation_metadata_col_name = df_pys_list_esquema_tbl_origen.where(F.col("COLUMN_NAME") == nombre_campo_tecnico)
    df_query_validation_metadata_data_type = df_query_validation_metadata_col_name.where(F.col("DATA_TYPE") == tipo_dato)
    df_query_validation_metadata_nullable = df_query_validation_metadata_col_name.where(F.col("NULLABLE") == valor_nulo)
    df_query_validation_metadata_data_len = df_query_validation_metadata_col_name.where(F.col("DATA_LENGTH") == tipo_dato_longitud)
    df_query_validation_metadata_data_precision = df_query_validation_metadata_col_name.where(F.col("DATA_PRECISION") == tipo_dato_precision)
    df_query_validation_metadata_data_scale =  df_query_validation_metadata_col_name.where(F.col("DATA_SCALE") == tipo_dato_escala)
    
    col_name = df_query_validation_metadata_col_name.select(F.col('COLUMN_NAME')).collect()[0]['COLUMN_NAME'] if df_query_validation_metadata_col_name.count() > 0 else "' '"
    col_data_type = df_query_validation_metadata_data_type.select(F.col('DATA_TYPE')).collect()[0]['DATA_TYPE'] if df_query_validation_metadata_data_type.count() > 0 else "' '"
    col_null_value = df_query_validation_metadata_nullable.select(F.col('NULLABLE')).collect()[0]['NULLABLE'] if df_query_validation_metadata_nullable.count() > 0 else "' '"
    col_char_length = df_query_validation_metadata_data_len.select(F.col('CHAR_LENGTH')).collect()[0]['CHAR_LENGTH'] if df_query_validation_metadata_data_len.count() > 0 else "' '"
    col_data_precision = df_query_validation_metadata_data_precision.select(F.col('DATA_PRECISION')).collect()[0]['DATA_PRECISION'] if df_query_validation_metadata_data_precision.count() > 0 else "' '"
    col_data_scale = df_query_validation_metadata_data_scale.select(F.col('DATA_SCALE')).collect()[0]['DATA_SCALE'] if df_query_validation_metadata_data_scale.count() > 0 else "' '"
    
    # 4. Formateo para variables de salida {'is_valid': isValid, 'msg_error': msg_error}
    
    if col_name == "' '":
      msg_error = msg_error + "* Descripcion: El valor de COLUMN_NAME y NombreCampoTecnico no coinciden: " + str(col_name) + ", " + str(nombre_campo_tecnico) +  " [Columna]: " + str(col_name) +  " \n"
      isValid = False
    elif col_data_type == "' '":
      msg_error = msg_error + "* Descripcion: El valor de DATA_TYPE y TipoDato no coinciden: " + str(col_data_type) + ", " + str(tipo_dato) + " [Columna]: " + str(col_name) + " \n"
      isValid = False
    elif col_null_value == "' '":
      msg_error = msg_error + "* Descripcion: El valor de NULLABLE y ValorNulo no coinciden: " + str(col_null_value) + ", " + str(valor_nulo) + " [Columna]: " + str(col_name) + " \n"
      isValid = False
    elif col_char_length == "' '":
      msg_error = msg_error + "* Descripcion: El valor de CHAR_LENGTH y Longitud no coinciden: " + str(col_char_length) + ", " + str(tipo_dato_longitud) + " [Columna]: " + str(col_name) + " \n"
      isValid = False
    elif col_data_precision == "' '":
      msg_error = msg_error + "* Descripcion: El valor de DATA_PRECISION y Precision no coinciden: " + str(col_data_precision) + ", " + str(col_data_precision) + " [Columna]: " + str(col_name) +  " \n"
      isValid = False  
    elif col_data_scale == "' '":
      msg_error = msg_error + "* Descripcion: El valor de DATA_SCALE y Escala no coinciden: " + str(col_data_scale) + ", " + str(col_data_scale) + "[Columna]: " + str(col_name) + " \n"
      isValid = False
         
    msg_error = msg_error + '------------------------------------------------------------------------------------------\n'
    
  
  msg_error = "" if  isValid == True else msg_error
  #print(msg_error)
  return {'is_valid': isValid, 'msg_error': msg_error}
  

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC # FUNCION DE INICIALIZACION

# COMMAND ----------

def init():
  
  # 1. Conversion a objeto list_esquema_tbl_origen to list-json[]
  list_esquema_tbl_origen = esquema_tbl_origen[ 1:len(esquema_tbl_origen) - 1]
  json_list_esquema_tbl_origen = json.loads(list_esquema_tbl_origen)
  df_pys_list_esquema_tbl_origen = create_table_src_schema(json_list_esquema_tbl_origen)
    
  # 2. Get df Metadatos current table from Formato_ArchivoFuente_Generico.xlsx
  df_tbl_metadatos_usuario = get_table_src_schema()
  
  # 3. Get df table Inventory Tables BD from Formato_ArchivoFuente_Generico.xlsx
  df_tbl_metadatos_inventory_tables_usuario = get_table_src_inventory_tables()
     
  # 4. Compare count rows user Metadata vs Current Database Metadata (Compara camtidad de columnas BD VS Formato_ArchivoFuente_Generico.xlsx)
  dic_result_metadata_cout_rows = is_valid_metadata_count_rows(df_pys_list_esquema_tbl_origen, df_tbl_metadatos_usuario)
    
  # 5. Compare user Metadata vs Current Database Metadata 
  dic_result_metadata_valid = is_valid_metadata(df_pys_list_esquema_tbl_origen, df_tbl_metadatos_usuario)
    
  # 6. Compare count rows user inventory tables vs select count(*) from currentExtractTable
  dic_result_metadata_count_rows_current_tbl = is_valid_metadata_count_rows_current_tbl(df_tbl_metadatos_inventory_tables_usuario) 
      
  # 7. Asignar estado de la validación
  is_current_tbl_metadata_valid = True if dic_result_metadata_cout_rows['is_valid'] and dic_result_metadata_valid['is_valid'] and dic_result_metadata_count_rows_current_tbl['is_valid'] else False
  
  print(is_current_tbl_metadata_valid)
                                                                                            
  list_results = [
    {'dic_result_metadata_cout_rows': dic_result_metadata_cout_rows},
    {'dic_result_metadata_valid': dic_result_metadata_valid},
    {'dic_result_metadata_count_rows_current_tbl': dic_result_metadata_count_rows_current_tbl}  
  ]
  
  print(list_results)
  return is_current_tbl_metadata_valid, list_results

# COMMAND ----------

is_current_tbl_metadata_valid, list_results = init()

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC ### RETORNO - OUTPUT NOTEBOOK

# COMMAND ----------

"""
Descripción: Retorna valor del resultado de las validaciones de los Metadatos:

1. Metadatos de usuario vs Describe TBL DB
2. Cantidad de registros registrados por usuario vs COUINT(*) TBL BD
3. Cantidad de Columnas de esquema tables registrados por usuario vs COUNT(COLS) Describe TBL DB
"""

import json

dbutils.notebook.exit(json.dumps({
  "is_current_tbl_metadata_valid": is_current_tbl_metadata_valid,
  "list_result_validations": list_results  
}))