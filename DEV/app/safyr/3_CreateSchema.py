# Databricks notebook source
from pyspark.sql.functions import col, substring, row_number, concat, monotonically_increasing_id, lit, when

# COMMAND ----------

"""Parametros enviado desde Factory """

dbutils.widgets.text("CarpetaBlob", "","")
dbutils.widgets.get("CarpetaBlob")
rowKey = getArgument("CarpetaBlob")


if rowKey.count('/') > 0:
  schema, application = rowKey.split('/') 
else: 
  application = rowKey.lower()

# COMMAND ----------

#Variables Globales Staticas

file_location = '/mnt/app-' + application + '/'
zona_crudos = '/raw/'
zona_result = 'results/'
name_table = 'zdr_'

tbl_metadatos =  application + '.zdr_' + application + '_src_metadatos'


# CSV options
infer_schema = True
first_row_is_header = True
delimiter = "|"
file_type = "csv"

#separadar de resultados
sep_line = 100
query_result = 'SELECT '
nombre_tbl_app_origen_tmp = 'zdc_' + application + '_temp'
flag = 'Si'

# COMMAND ----------

def SaveFileDescribeTable(prm_nombre_tbl_app_origen_tmp):
  try:
    path_write = '/mnt/app-' + application + '-storage/'
    dbutils.fs.rm(path_write + 'metadata_' + application + '_db.txt', True)
    meta = sqlContext.sql('describe ' + prm_nombre_tbl_app_origen_tmp)
    pmeta = meta.select("*").toPandas()
    pmeta.to_csv('/dbfs' + path_write + 'metadata_' + application + '_db.txt', index=False, columns=['col_name', 'data_type'], sep='|')
  
    print('Archivo describe de la metadata creado correctamente')
  except Exception as error:
    print('No se creo el archivo metadata' + str(error))

# COMMAND ----------

def getObjectsCurrentMetadata(prm_flag):
  
  sqlContext.sql('REFRESH TABLE ' + tbl_metadatos)
  query_metadata = 'SELECT * FROM ' +  tbl_metadatos + ' WHERE EsMigrable = ' + "'" + prm_flag  + "'" + ' and EsVisible = ' + "'" + prm_flag  + "'"
  df_current_metadata = sqlContext.sql(query_metadata)


  lista_campos_visualizar_principal = df_current_metadata.filter((col('FechaBusqueda')  == 'No') &  
                                                                 (col('TablaDestino') == 'zdr_safyr_panel_principal')
                                                                 ).select("NombreCampoTecnico","NombreCampoFuncional"
                                                                 ).collect()
  
  lista_campos_visualizar_detalle = df_current_metadata.filter((col('FechaBusqueda')  == 'No') &  
                                                               (col('TablaDestino') == 'zdr_safyr_panel_detalle')
                                                               ).select("NombreCampoTecnico", "NombreCampoFuncional"
                                                               ).collect()
  
  lista_campos_busqueda = df_current_metadata.filter("EsCampoBusqueda  == 'Si'").select("NombreCampoTecnico",
                                                                                        "NombreCampoFuncional").collect()
  
  return df_current_metadata, lista_campos_visualizar_principal, lista_campos_visualizar_detalle , lista_campos_busqueda

# COMMAND ----------

def getObjectsListSearchFiles(lista_campos_visualizar_principal, lista_campos_visualizar_detalle, prm_lista_campos_busqueda):
  
  #genere campos para Panel Principal
  campos_lista_principal = [(i.NombreCampoTecnico + " AS " + i.NombreCampoFuncional) for i in lista_campos_visualizar_principal]
  campos_str_principal = ', '.join([str(x) for (i, x) in enumerate(campos_lista_principal)])
  
  #genere campos para Panel Detalle
  campos_lista_detalle = [(i.NombreCampoTecnico + " AS " + i.NombreCampoFuncional) for i in lista_campos_visualizar_detalle]
  campos_str_detalle = ', '.join([str(x) for (i, x) in enumerate(campos_lista_detalle)])
  
  #genera campos para la Busqueda
  nombre_campo_tecnico_lista = [str(i.NombreCampoTecnico) for i in prm_lista_campos_busqueda]
  campos_consulta_str = ', '.join([str(x) if i == len(nombre_campo_tecnico_lista) else "COALESCE(" + str(x) + ", ''" + ") ,' - '" for (i, x) in enumerate(nombre_campo_tecnico_lista)])  
  
  return campos_str_principal, campos_str_detalle , campos_consulta_str

# COMMAND ----------

def getCalculatedFields(prm_df_current_metadata, prm_query_result, prm_nombre_tbl_app_origen_tmp):
  
  df_current_metadata_campos_busqueda_fecha = prm_df_current_metadata.filter(col('FechaBusqueda')  == 'Si').select("NombreCampoTecnico",
                                                                                                                   "NombreCampoFuncional",
                                                                                                                   "Formato")
  campo_fecha_tecnico = df_current_metadata_campos_busqueda_fecha.first()["NombreCampoTecnico"]  
  campo_fecha_funcional = df_current_metadata_campos_busqueda_fecha.first()["NombreCampoFuncional"]  
  campo_dia  = 'CAST(SUBSTR(' + campo_fecha_tecnico + ', 9, 2) AS STRING) DIA_CALCULADO_RDH'
  campo_mes  = 'CAST(SUBSTR(' + campo_fecha_tecnico + ', 6, 2) AS STRING) MES_CALCULADO_RDH'
  campo_anio = 'CAST(SUBSTR(' + campo_fecha_tecnico + ', 0, 4) AS STRING) ANIO_CALCULADO_RDH'
  
  campo_dim_tiempo = 'CAST(CONCAT(SUBSTR(' + campo_fecha_tecnico + ', 9, 2), SUBSTR(' + campo_fecha_tecnico +   ', 6, 2), SUBSTR(' + campo_fecha_tecnico + ' , 0, 4)) AS STRING) AS FECHA_DIM_TIEMPO_CALCULADO_RDH'
  prm_query_result = prm_query_result + ', ' + 'CAST(' + campo_fecha_tecnico + ' AS DATE) AS ' + campo_fecha_funcional + ', ' + campo_anio + ', ' + campo_mes + ', ' + campo_dia + ', ' + campo_dim_tiempo + ' from ' + prm_nombre_tbl_app_origen_tmp
  
  return prm_query_result

# COMMAND ----------

def materializeTablZdr(prm_query_result_principal, prm_query_result_detalle, prm_campo_indentificador_busqueda):
  
  #1. Crear Base de de Datos
  sqlContext.sql('create database if not exists ' + application)
  
  #2. Materializar tabla en HIVE Panel Principal
  df_current_tbl_data_lake_principal = sqlContext.sql(prm_query_result_principal)
  name_table_app = application + '.' + name_table + application

  # 2.1 crear particionamiento y formato tipo delta para optimizar consultas panel principal
  sqlContext.sql('drop table if exists ' + name_table_app + '_panel_principal')
  df_current_tbl_data_lake_principal.repartition("ANIO_CALCULADO_RDH", "MES_CALCULADO_RDH"
                                                 ).write.format("delta"
                                                 ).mode("overwrite"
                                                 ).partitionBy("ANIO_CALCULADO_RDH", "MES_CALCULADO_RDH"       
                                                 ).option('path', file_location + zona_result + 'panel_principal'
                                                 ).saveAsTable(name_table_app + '_panel_principal')
  
  sqlContext.sql('REFRESH TABLE ' + name_table_app + '_panel_principal')
  print('Se materializa la tabla: ' + name_table_app + '_panel_principal')
  
  query_index_principal = 'OPTIMIZE ' + name_table_app + '_panel_principal' + ' ZORDER BY (' + prm_campo_indentificador_busqueda + ')'
  sqlContext.sql(query_index_principal)
  print('Se crea el Indice : ' + name_table_app + '_panel_principal' + ' en el campo: ' + prm_campo_indentificador_busqueda + '\n')
    
  #3. Materializar tabla en HIVE Panel Detalle
  df_current_tbl_data_lake_detalle = sqlContext.sql(prm_query_result_detalle)

  #3.1 crear particionamiento y formato tipo delta para optimizar consultas panel detalle
  sqlContext.sql('drop table if exists ' + name_table_app + '_panel_detalle')
  df_current_tbl_data_lake_detalle.repartition("ANIO_CALCULADO_RDH", "MES_CALCULADO_RDH"
                                                 ).write.format("delta"
                                                 ).mode("overwrite"
                                                 ).partitionBy("ANIO_CALCULADO_RDH", "MES_CALCULADO_RDH"
                                                 ).option('path',  file_location + zona_result + 'panel_detalle'
                                                 ).saveAsTable(name_table_app + '_panel_detalle')

  sqlContext.sql('REFRESH TABLE ' + name_table_app + '_panel_detalle')
  print('Se materializa la tabla: ' + name_table_app + '_panel_detalle')
  
  query_index_detalle = 'OPTIMIZE ' + name_table_app + '_panel_detalle' + ' ZORDER BY (' + prm_campo_indentificador_busqueda + ')'
  sqlContext.sql(query_index_detalle)
  print('Se crea el Indice : ' + name_table_app + '_panel_detalle' + ' en el campo: ' + prm_campo_indentificador_busqueda)

# COMMAND ----------

def procesar_dinamyc_tables():
  #Lee el CSV de la zona de dato crudos
  df_current_tbl_raw = spark.read.csv(file_location + zona_crudos, header = first_row_is_header, sep = delimiter)
  df_current_tbl_raw.registerTempTable(nombre_tbl_app_origen_tmp)

  SaveFileDescribeTable(nombre_tbl_app_origen_tmp)

  #Devuelve los campos que se van a usar para Panel Principal, Panel Detalle y Campo de Busqueda
  df_current_metadata, lista_campos_visualizar_principal, lista_campos_visualizar_detalle,  lista_campos_busqueda = getObjectsCurrentMetadata(flag)

  #Recupera el campo de filtro para la busqueda
  campo_indentificador_busqueda = df_current_metadata.select('NombreCampoTecnico', 'NombreCampoFuncional'
                                                            ).filter(col('EsCampoBusqueda') == 'Si'
                                                            ).sort(col('NombreCampoFuncional'), ascending = False).first()

  #Devuelve los campos que se van a usar para Panel Principal, Panel Detalle y Campo de Busqueda para ejecuar la sentencia SQL
  campos_str_principal, campos_str_detalle, campos_consulta_str = getObjectsListSearchFiles(lista_campos_visualizar_principal, 
                                                                                            lista_campos_visualizar_detalle, 
                                                                                            lista_campos_busqueda)

  
  query_result_principal = query_result + campos_str_principal + ", translate(lower(concat(" + campos_consulta_str + ")),'áéíóúÁÉÍÓÚñÑü','aeiouAEIOUnNu') AS busqueda"
  query_result_detalle = query_result + campos_str_detalle + ', ' + str(campo_indentificador_busqueda['NombreCampoTecnico']) + ' as ' + str(campo_indentificador_busqueda['NombreCampoFuncional'])
  
  query_result_principal = getCalculatedFields(df_current_metadata, query_result_principal, nombre_tbl_app_origen_tmp)
  query_result_detalle = getCalculatedFields(df_current_metadata, query_result_detalle, nombre_tbl_app_origen_tmp)

  #Materializa tabla ZDC en la tabla de la ZDR, formato delta, particiona por año y mes, agrega un indice por el campo identificador.
  materializeTablZdr(query_result_principal, query_result_detalle, campo_indentificador_busqueda['NombreCampoFuncional'])

# COMMAND ----------

procesar_dinamyc_tables()
