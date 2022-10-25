# Databricks notebook source
# MAGIC %md
# MAGIC # EXTRACCION Y CREACION DE TABLAS METADATOS

# COMMAND ----------

# DBTITLE 1,Importación de librerías
import pandas as pd
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Mapeo de variables
"""Path Ruta de la Aplicacion"""
# Ejemplo ejecucion: folder_blob = app-semih/metadatos
dbutils.widgets.text("folder_blob", "","")
dbutils.widgets.get("folder_blob")
folder_blob = getArgument("folder_blob")
file_metadata_name = '/Formato_ArchivosFuente_Generico.xlsm'               ## Recibir como parametro ojoooooooooooooooooooooooooooooooooooooooooo

application = folder_blob.split('/')[0]
sub_folder_blob = folder_blob.split('/')[1]
application_db = application.split('-')[1]

path_app = '/mnt/' + application
file_path_formato_fuente_generico = '/dbfs/mnt/' + application + '-storage/' + sub_folder_blob + file_metadata_name

# COMMAND ----------

# DBTITLE 1,CREAR BD Y DEFINIR VARIABLES DINÁMICAS
sqlContext.sql('CREATE DATABASE IF NOT EXISTS ' + application_db)

tbl_prm_metadatos = 'Zdr_' + application_db + '_Src_Metadatos_Params'
tbl_metadatos = 'Zdr_' + application_db + '_Src_Metadatos'
tbl_inventarios_tbls_bd = 'Zdr_' + application_db + '_Src_Metadatos_Inventario_Tablas_BD'

top = '100'
tipo_bd_origen = ''
prm_ambiente = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
prm_ambiente = prm_ambiente[prm_ambiente.find('@') + 1 : prm_ambiente.find('.')]
prm_ambiente = prm_ambiente.strip().upper()
dominio_dev = 'AMBIENTESBC'
dominio_cert = 'AMBIENTESBC'
dominio_prod = 'BANCOLOMBIA'

tipos_bd_origen = {
                      'ORACLE':     {'BD' : 'ORACLE',
                                     'TOP': 'WHERE rownum <= ' + top,
                                     'TIPO_SENTENCIA': 'WHERE',
                                     'FORMAT_DATE' : '\"' + "  TO_CHAR(" + "NombreCampoTecnico" + ", 'YYYY-MM-DD HH24:MI:SS')" + '\"'},
                      'SQL_SERVER': {'BD' : 'MICROSOFT SQL SERVER',
                                     'TOP': 'SELECT TOP ' + top,
                                     'TIPO_SENTENCIA': 'SELECT',
                                     'FORMAT_DATE' : '\"' + 'CONVERT(varchar, NombreCampoTecnico, 21) as [YYYY-MM-DD HH:MM:SS.mmm]' + '\"'},
                      'DB2':        {'BD' : 'DB2',
                                     'TOP': 'LIMIT ' + top,
                                     'TIPO_SENTENCIA': 'LIMIT',
                                     'FORMAT_DATE' : '\"' + "  TO_CHAR(" + "NombreCampoTecnico" + ", 'YYYY-MM-DD HH24:MI:SS')" + '\"'},
                      'HIVE':       {'BD' : 'HIVE',
                                     'TOP': 'LIMIT ' + top,
                                     'TIPO_SENTENCIA': 'LIMIT',
                                     'FORMAT_DATE' : '\"' + "  TO_CHAR(" + "NombreCampoTecnico" + ", 'YYYY-MM-DD HH24:MI:SS')" + '\"'},
                      'IMPALA':     {'BD' : 'IMPALA',
                                     'TOP': 'LIMIT ' + top,
                                     'TIPO_SENTENCIA': 'LIMIT',
                                     'FORMAT_DATE' : '\"' + "  TO_CHAR(" + "NombreCampoTecnico" + ", 'YYYY-MM-DD HH24:MI:SS')" + '\"'},
                      'POSTGRESSQL':{'BD' : 'POSTGRESSQL',
                                     'TOP': 'LIMIT ' + top,
                                     'TIPO_SENTENCIA': 'LIMIT',
                                     'FORMAT_DATE' : '\"' + "  TO_CHAR(" + "NombreCampoTecnico" + ", 'YYYY-MM-DD HH24:MI:SS')" + '\"'},
                      'MYSQL':      {'BD' : 'MYSQL',
                                     'TOP': 'LIMIT ' + top,
                                     'TIPO_SENTENCIA': 'LIMIT',
                                     'FORMAT_DATE' : '\"' + "  TO_CHAR(" + "NombreCampoTecnico" + ", 'YYYY-MM-DD HH24:MI:SS')" + '\"'}
                   }

# COMMAND ----------

"""
Descripción: Reseteo de rutas delta para la creación de las tablas de metadatos.  
Autor: Juan Escobar.
Fecha: 08/01/2021
"""

def reset_path_metadata_tbls():
 
  list_paths_meta_tbables = ["/mnt/" + application + "/results/panel_src_metadatos_parametros", 
                             "/mnt/" + application + "/results/panel_src_metadatos",
                             "/mnt/" + application + "/results/panel_src_metadatos_inventario_tablas_bd"]
  
  for i in list_paths_meta_tbables:
    print(i)
    dbutils.fs.rm(i, True)

# COMMAND ----------

"""
Descripción: Obtiene la tabla parametros tipo Dataframe (DataTable) a partir del archivo Formato_ArchivosFuente_Generico.xlsm
             Hoja = BD.
             https://stackoverflow.com/questions/43367805/pandas-read-excel-multiple-tables-on-the-same-sheet   
Autor: Juan Escobar.
Fecha:29/07/2020
"""

def get_df_params_tables():
 
  sheet = 'BD'
  xl = pd.ExcelFile(file_path_formato_fuente_generico, sheet_name = sheet,index_col = 0)
  nrows = xl.book.sheet_by_name(sheet).nrows
  df_parametros = xl.parse(sheetname = sheet, header = 0, skipfooter = nrows  - (29)).dropna(axis=1, how='all')
   
  return df_parametros

# COMMAND ----------

"""
Descripción: Obtiene la tabla metadatos tipo Dataframe (DataTable) a partir del archivo Formato_ArchivosFuente_Generico.xlsx
             Hoja = BD.
             https://stackoverflow.com/questions/43367805/pandas-read-excel-multiple-tables-on-the-same-sheet   
Autor: Juan Escobar.
Fecha:29/07/2020
"""

def get_df_metadata_tables(prm_skiprows, prm_rows_metadata_table):
 
  sheet = 'BD'
  xl = pd.ExcelFile(file_path_formato_fuente_generico, sheet_name = sheet,index_col = 0)
  nrows = xl.book.sheet_by_name(sheet).nrows    
  df_tbl_metadata = xl.parse(sheetname = sheet, skiprows = prm_skiprows, skipfooter = nrows  - (prm_rows_metadata_table)).dropna(axis=1, how='all')
 
  return df_tbl_metadata

# COMMAND ----------

def get_row_key_value(df_row):  
  nomb_app = str(df_row['NombreAplicacion']).strip().upper()
  nomb_sub_app = str(df_row['NombreSubAplicacion']).strip().upper()
  nomb_tbl_app = str(df_row['NombreTablaAplicacion']).strip().upper()
   
  row_key = nomb_app + '-' + nomb_sub_app + '-' + nomb_tbl_app 
  return row_key

# COMMAND ----------

"""
Descripción: Obtiene dos objetos tipo Dataframe (DataTable) a partir del archivo Formato_ArchivosFuente_Generico.xlsx.
             Hoja = BD Tables.
Autor: Juan Escobar.
Fecha creación: 30/07/2020.
Fecha modificación: 13/01/2021.
Modificación: Se elimina display por no compatibilidad con Dataframe Pandas
TODO: Validar si el campo es migrable.
"""

def get_df_inventory_bd_tables(prm_skipfooter):

  sheet = 'InventarioTablas'
  xl = pd.ExcelFile(file_path_formato_fuente_generico, sheet_name = sheet, index_col = 0)
  nrows = xl.book.sheet_by_name(sheet).nrows
  df_tables_bd = xl.parse(sheetname = sheet, header = 0, skipfooter = nrows  - (prm_skipfooter)).dropna(axis=1, how='all')
  df_tables_bd['row_key'] = df_tables_bd.apply(get_row_key_value, axis=1)
  
  return df_tables_bd

# COMMAND ----------

"""
Descripción: Retorna sentencia top de acuerdo al tipo de BD 
Autor: Juan Escobar.
Fecha: 30/09/2020.
Parámetros: prm_tipo_bd, prm_ambiente
"""

def get_query_select_top(sql, tipo_bd):
  
  top_sql = tipos_bd_origen[tipo_bd]['TOP']
  tipo_sentencia = tipos_bd_origen[tipo_bd]['TIPO_SENTENCIA']
  
  if tipos_bd_origen[tipo_bd]['TIPO_SENTENCIA'] == 'WHERE' or tipos_bd_origen[tipo_bd]['TIPO_SENTENCIA'] == 'LIMIT': 
    sql = sql + ' ' + top_sql
  elif tipos_bd_origen[tipo_bd]['TIPO_SENTENCIA'] == 'SELECT':
    sql = sql.replace('SELECT', top_sql)
  
  return sql

# COMMAND ----------

"""
Descripción: Generación de Query Dinamico de cada tabla de la BD, incluye validación que agrega unicamente las columnas que se deben migrar.
Autor: Juan Escobar.
Fecha: 30/07/2020.
"""

def create_df_bd_tables_dynamic_query(df_tbl_metadata_pys, df_tables_bd, tipo_bd_origen):
   
  df_tables_bd['Query'] = ''
  df_tables_bd['QueryCount'] = ''
    
  for index, row in df_tables_bd.iterrows():
    query_fields = 'SELECT '     
    esquema = row['Esquema']    
    
    nombre_tabla_aplicacion = row['NombreTablaAplicacion'].upper().strip()
    sub_application_current = row['NombreSubAplicacion'].upper().strip()
    queryCountRows = query_fields + 'COUNT(*) FROM ' + esquema + '.' + nombre_tabla_aplicacion
    
    df_tbl_metadata_pys_current_tbl = df_tbl_metadata_pys.where((F.trim(F.upper(F.col("TablaOrigen"))) == nombre_tabla_aplicacion))
    df_tbl_metadata_pys_current_tbl.createOrReplaceTempView("df_tbl_metadata_pys_current_tbl")
    
    formats_dates_tipos_bd = {}
        
    #format_date_rdh = '\"' + "  TO_CHAR(" + "NombreCampoTecnico" + ", 'YYYY-MM-DD HH24:MI:SS')" + '\"'
    format_date_rdh = tipos_bd_origen[tipo_bd_origen]['FORMAT_DATE']
    
    query = "SELECT CASE WHEN TipoDato = 'DATE' THEN " + format_date_rdh + " WHEN TipoDato <> 'DATE' THEN NombreCampoTecnico " \
            "END AS NombreCampoTecnicoCalculado, "\
            "NombreCampoTecnico "\
            "FROM "\
            "df_tbl_metadata_pys_current_tbl WHERE EsMigrable = 'Si' "
        
    current_Table = sqlContext.sql(query)     
    current_Table.createOrReplaceTempView("df_tbl_current_Table")
    
    query = "SELECT regexp_replace(NombreCampoTecnicoCalculado,'NombreCampoTecnico', NombreCampoTecnico) as NombreCampoTecnicoCalculado, "\
            "NombreCampoTecnico "\
            "FROM df_tbl_current_Table"
    
    current_Table = sqlContext.sql(query)      
    list_fields = current_Table.select(F.col('NombreCampoTecnicoCalculado'), F.col('NombreCampoTecnico')).collect() 
    nombre_campo_array = [str(i.NombreCampoTecnicoCalculado + " as "  + i.NombreCampoTecnico) for i in list_fields]   
    query_fields = query_fields + ','.join([str(elem) for elem in nombre_campo_array]) + ' ' + 'FROM ' + esquema + '.' + row['NombreTablaAplicacion']  
    
    # select top para ambientes pre productivos (AMBIENTESBC)   
    query_fields = get_query_select_top(query_fields, tipo_bd_origen) if (prm_ambiente == dominio_dev or prm_ambiente == dominio_cert) else query_fields
        
    df_tables_bd.at[index, 'Query'] = str(query_fields)
    df_tables_bd.at[index, 'QueryCount'] = str(queryCountRows)
        
  return df_tables_bd

# COMMAND ----------

"""
Descripción: Traspone tabla parametros de archivo Metadatos, convierte un dataframe de tipo pyspark con columnas renombradas
             por medio de alias invocables.
Autor: Juan Escobar.
Fecha: 06/08/2020.
"""

def get_df_formated_params(df_parametros): 
  df1_transposed = df_parametros.T

  headers = df1_transposed.iloc[0]
  df_parametros  = pd.DataFrame(df1_transposed.values[1:], columns = headers)

  df_parametros_pys = sqlContext.createDataFrame(df_parametros.astype(str))
  df_parametros_pys_format = df_parametros_pys.select(F.col('Nombre Fuente de Datos').alias('NombreFuenteDatos'), 
                                                      F.col('Tipo BD').alias('TipoBD'), 
                                                      F.col('Versión del documento').alias('VersionDocumento'), 
                                                      F.col('Fuente').alias('Fuente'), 
                                                      F.col('Fecha Creación (DD/MM/YYYY)').alias('FechaCreacion'), 
                                                      F.col('Fecha Modificación (DD/MM/YYYY)').alias('FechaModificacion'), 
                                                      F.col('Autor Modificación').alias('AutorModificacion'), 
                                                      F.col('Descripción Modificación').alias('DescripcionModificacion'), 
                                                      F.col('Responsable Tecnico').alias('ResponsableTecnico'), 
                                                      F.col('Cantidad de Archivos Fuente').alias('CantidadArchivosFuente'), 
                                                      F.col('Origen').alias('Origen'), 
                                                      F.col('Extensión del Archivo').alias('ExtensionArchivo'), 
                                                      F.col('Catidad de carpetas (Adjuntos)').alias('CatidadCarpetasAdjuntos'), 
                                                      F.col('Versión Estructura Fuente').alias('VersionEstructuraFuente'), 
                                                      F.col('Codificación Archivo').alias('CodificacionArchivo'),
                                                      F.col('Responsable Funcional').alias('ResponsableFuncional'),
                                                      F.col('Cantidad Máxima Usuarios Concurrentes').alias('UsuariosConcurrentes'),
                                                      F.col('Semáforo de datos').alias('SemaforoDatos'),
                                                      F.col('Proyecto').alias('Proyecto'),
                                                      F.col('Linea de Negocio').alias('LineaNegocio'),
                                                      F.col('Usuario Tecnico Responsable').alias('UsuarioTecnicoResponsable'),
                                                      F.col('Gernete Dueño').alias('GerneteDuennio'),
                                                      F.col('Indice Nro Columna Tabla (InventarioTablas)').alias('ColsInventarioTablas'),
                                                      F.col('Indice Nro Filas Tabla (InventarioTablas)').alias('FilasInventarioTablas'),
                                                      F.col('Indice Nro Columnas Tabla (TablaParametros)').alias('ColsTablaParametros'),
                                                      F.col('Indice Nro Filas Tabla (TablaParametros)').alias('FilasTablaParametros'),
                                                      F.col('Indice Nro Columnas Tabla (TablaMetadatos)').alias('ColsTablaMetadatos'),
                                                      F.col('Indice Nro Filas Tabla (TablaMetadatos)').alias('FilasTablaMetadatos'))
   
  return df_parametros_pys_format

# COMMAND ----------

def init_data_frames():    
  
  # 0.Reset path tables metadata.
  reset_path_metadata_tbls()
  
  # 1.Leer tablas insumo archivo Metadatos - Formato_ArchivosFuente_Generico.xlsx
  df_parametros = get_df_params_tables()
    
  # 2. Obtener tabla traspuesta de df_parametros
  df_parametros_pys_formated = get_df_formated_params(df_parametros)
    
  # 2.1 Obtener los indices filas y columnas de las tablas del excel Formato_ArchivosFuente_Generico.xlsx 
  cols_inventario_tablas = int(df_parametros_pys_formated.select('ColsInventarioTablas').collect()[0]['ColsInventarioTablas']) 
  filas_inventario_tablas = int(df_parametros_pys_formated.select('FilasInventarioTablas').collect()[0]['FilasInventarioTablas'])
  
  cols_tabla_parametros = int(df_parametros_pys_formated.select('ColsTablaParametros').collect()[0]['ColsTablaParametros'])
  filas_tabla_parametros = int(df_parametros_pys_formated.select('FilasTablaParametros').collect()[0]['FilasTablaParametros'])
  
  cols_tabla_metadatos = int(df_parametros_pys_formated.select('ColsTablaMetadatos').collect()[0]['ColsTablaMetadatos'])
  filas_tabla_metadatos = int(df_parametros_pys_formated.select('FilasTablaMetadatos').collect()[0]['FilasTablaMetadatos'])
  
  # 2.2 Habilitar almacenado formato delta.
  sqlContext.sql("SET spark.databricks.delta.formatCheck.enabled=false")
  
  # 2.3 Registrar datos en ADL para tabla fisica parametros metadatos
  print("/mnt/" + application + "/results/panel_src_metadatos_parametros")
  sqlContext.sql('DROP TABLE IF EXISTS ' + application_db +   '.' + tbl_prm_metadatos)  
  df_parametros_pys_formated.write.format("delta"
                                         ).mode("overwrite").save("/mnt/" + application + "/results/panel_src_metadatos_parametros")
      
  # 3. Registrar datos en ADL para tabla fisica metadatos
  sqlContext.sql('DROP TABLE IF EXISTS ' + application_db + '.' + tbl_metadatos)
  df_tbl_metadata = get_df_metadata_tables(filas_tabla_parametros + 1, filas_tabla_parametros + filas_tabla_metadatos + 1)  
  df_tbl_metadata_pys = sqlContext.createDataFrame(df_tbl_metadata.astype(str))
  df_tbl_metadata_pys.write.format("delta"
                                  ).mode("overwrite"
                                  ).save("/mnt/" + application + "/results/panel_src_metadatos")
                                     
  # 4. Generar columna con query de extracción dinamica, registrar datos en ADL tabla fisica con inventario de tablas.
  tipo_bd_origen = df_parametros_pys_formated.first()["TipoBD"]
  sqlContext.sql('DROP TABLE IF EXISTS ' + application_db + '.' + tbl_inventarios_tbls_bd)
  df_tables_bd = get_df_inventory_bd_tables(filas_inventario_tablas)
  df_result_query_dynamic = create_df_bd_tables_dynamic_query(df_tbl_metadata_pys, df_tables_bd, tipo_bd_origen)
  df_result_query_dynamic = sqlContext.createDataFrame(df_result_query_dynamic.astype(str))
  df_result_query_dynamic.write.format("delta"
                                      ).mode("overwrite"
                                      ).save("/mnt/" + application + "/results/panel_src_metadatos_inventario_tablas_bd") 
  
  return df_parametros_pys_formated, df_tbl_metadata_pys, df_result_query_dynamic, tipo_bd_origen

# COMMAND ----------

# DBTITLE 1,Init()
df_prm_meta, df_meta, df_inventario, tipo_bd_origen = init_data_frames()

# COMMAND ----------

# DBTITLE 1,Repisar valor
tbl_prm_metadatos = 'Zdr_' + application_db + '_Src_Metadatos_Params'
tbl_metadatos = 'Zdr_' + application_db + '_Src_Metadatos'
tbl_inventarios_tbls_bd = 'Zdr_' + application_db + '_Src_Metadatos_Inventario_Tablas_BD'

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC ### PANEL PARAMETROS METADATOS

# COMMAND ----------

# 1. Borrar tabla si existe
sqlContext.sql('DROP TABLE IF EXISTS ' + application_db +   '.' + tbl_prm_metadatos)

# 2. Escribir tabla
df_prm_meta.write.mode("overwrite").option('path', '/mnt/' + application + '/results/panel_src_metadatos_parametros/').saveAsTable(application_db + '.' + tbl_prm_metadatos) 

# Refresh table
sqlContext.sql('REFRESH TABLE ' + application_db +   '.' + tbl_prm_metadatos)

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC ### PANEL METADATOS

# COMMAND ----------

# 1. Borrar tabla si existe
sqlContext.sql('DROP TABLE IF EXISTS ' + application_db + '.' + tbl_metadatos)

# 2. Escribir tabla
df_meta.write.mode("overwrite").option('path', '/mnt/' + application + '/results/panel_src_metadatos/').saveAsTable(application_db + '.' + tbl_metadatos)

# Refresh table
sqlContext.sql('REFRESH TABLE ' + application_db +   '.' + tbl_metadatos)

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC ### PANEL INVETANTARIO TABLAS FUENTE BD

# COMMAND ----------

# 1. Borrar tabla si existe
sqlContext.sql('DROP TABLE IF EXISTS ' + application_db + '.' + tbl_inventarios_tbls_bd)

# 2. Escribir tabla
df_inventario.write.mode("overwrite").option('path', '/mnt/' + application + '/results/panel_src_metadatos_inventario_tablas_bd/').saveAsTable(application_db + '.' + tbl_inventarios_tbls_bd)

# Refresh table
sqlContext.sql('REFRESH TABLE ' + application_db +   '.' + tbl_inventarios_tbls_bd)

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC ### VALIDACIONES

# COMMAND ----------

sqlContext.sql('REFRESH TABLE ' + application_db + '.' + tbl_prm_metadatos)
df1 = sqlContext.sql('SELECT * FROM ' + application_db + '.' + tbl_prm_metadatos)
display(df1)

# COMMAND ----------

sqlContext.sql('REFRESH TABLE ' + application_db + '.' + tbl_metadatos)
df2 = sqlContext.sql('SELECT * FROM ' + application_db + '.' + tbl_metadatos)
display(df2)

# COMMAND ----------

sqlContext.sql('REFRESH TABLE ' + application_db + '.' + tbl_inventarios_tbls_bd)
df3 = sqlContext.sql('SELECT * FROM ' + application_db + '.' + tbl_inventarios_tbls_bd)
display(df3)

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC ### RETORNO - OUTPUT NOTEBOOK

# COMMAND ----------

# DBTITLE 1,ALMACENAR RETORNO TBL AUX
"""
Descripción: Almacena los valores de salida del pipeline 4-CreateMetadataTables en una tabla auxiliar
             para ser utilizados en otros pipelines de la ETL.
"""

# 1. Define vars To DF
prm_aux_tbl = 'PARAM_' + application_db.upper() + '_APP_HELP_PIPELINE_CREATE_TABLE'

tbl_aux_output_pipeline_create_metadata = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='" + prm_aux_tbl + "' LIMIT 1").first()["Valor1"]
print(tipo_bd_origen, prm_aux_tbl, tbl_aux_output_pipeline_create_metadata)

list_outputs = {'tbl_prm_metadatos': [application_db + '.' + tbl_prm_metadatos], 
                'tbl_metadatos': [application_db + '.' + tbl_metadatos],
                'tbl_inventarios_tbls_bd': [application_db + '.' + tbl_inventarios_tbls_bd],
                'tipo_bd_origen': [tipo_bd_origen],
                'application': [application_db],
                'sub_application': [sub_folder_blob],
                'dominio_actual': [prm_ambiente]
               }
keys = ['tbl_prm_metadatos', 'tbl_metadatos', 'tbl_inventarios_tbls_bd', 'tipo_bd_origen', 'application', 'sub_application', 'dominio_actual']

# 2. Create pandas DF
df_pd_list_current_outputs = pd.DataFrame (list_outputs, columns = keys)

# 3. Create pyspark DF
df_pys_list_current_outputs = sqlContext.createDataFrame(df_pd_list_current_outputs.astype(str))
df_pys_list_current_outputs.write.mode("overwrite").option('path', '/mnt/' + application + '/results/panel_aux_output_pipeline_create_metadata/').saveAsTable(application_db + '.' + tbl_aux_output_pipeline_create_metadata)
