# Databricks notebook source
# DBTITLE 1,CREACION TABLA AUD (Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata - LOG VALIDATIONS METADATA)
"""
Descripción: Log PreCarga: Se utiliza una tabla de log de auditoria para llevar la trazabilidad de los errores y hallazgos de la metadata registrada
             por el usuario dueño de la información y la fuente de datos tipo BD.
             Se utiliza en un paso previo a la extracción dinámica de la infromación con origen BD y destino Blob Storage (.Parquet), campos:
             
             -rowKey: Identificador unico de registro.
             -partitionKey: Identificador unico de registro resumido.
             -nombre_bd_origen: Nombre de la BD Origen a extraer.
             -nombre_tipo_bd_origen: Nombre del tipo de BD Origen a extraer.
             -nombre_app_origen: nombre de la BD / APP migrada.
             -nombre_tbl_sub_app_origen: Nombre de de la suaplicación en caso de que exista.
             -nombre_tbl_app_origen: Nombre de la tabla en BD Origen.
             -err_number_cols_table: Error generado al determinar un número diferente de columnas de los metadatos
              especificados en el archivo Formato_ArchivosFuente_Generico.xlsm y el describe de la tabla en la BD
              (Formato_ArchivosFuente_Generico.xlsx/Hoja[BD]).
              
             -err_metadata string: Error generado al determinar un diferencias entre los metadatos
              especificados en el archivo Formato_ArchivosFuente_Generico.xlsx y el describe de la tabla en la BD
              Formato_ArchivosFuente_Generico.xlsx/Hoja[BD].
              
             -err_number_rows_table: Error generado al determinar un diferencias entre los metadatos
              especificados en el archivo Formato_ArchivosFuente_Generico.xlsm y el select count(*) de la tabla en la BD
              Formato_ArchivosFuente_Generico.xlsx/Hoja[InventarioTablas].
             
             -fecha_registro:Fecha en la que se registro el log de aud.
             
             
Responsables: Juan David Escobar E.
Fecha Creación: 31/08/2020.
Fecha Modificación: 15/10/2020.
rowKey = llave primaria (nombre_app_origen + '-' + nombre_tbl_sub_app_origen + '-' + nombre_tbl_app_origen)
"""

def create_table_aud_metadata():

  sql_db_aud =  "CREATE DATABASE IF NOT EXISTS auditoria"
  sql_drop =  "DROP TABLE IF EXISTS auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata"
  sql_tbl_aud = "CREATE TABLE auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata \
  ( \
      rowKey string, \
      partitionKey string, \
      nombre_bd_origen string, \
      nombre_tipo_bd_origen string, \
      nombre_app_origen string, \
      nombre_sub_app_origen string, \
      nombre_esquema_tbl_origen string, \
      nombre_tbl_app_origen string, \
      err_number_cols_table string, \
      err_metadata string, \
      err_number_rows_table string, \
      fecha_registro timestamp \
  ) \
  USING DELTA \
  PARTITIONED BY (nombre_app_origen) \
  LOCATION '/mnt/rdh-auditoria/results/Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata'";
  
  sqlContext.sql(sql_db_aud)
  sqlContext.sql(sql_drop)
  sqlContext.sql(sql_tbl_aud)

# COMMAND ----------

# DBTITLE 1,CREACION TABLA AUD (Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob - COPY IN BLOB)
"""
Descripción: Log PosCarga: Se utiliza una tabla de log de auditoria para llevar la trazabilidad de los las tablas extraidas de la BD y copiadas en
             el BLOB STORAGE en formato .parquet
             Se utiliza en un paso posterior a la extracción dinámica (COPY TASK) de la infromación con origen BD y destino Blob Storage (.Parquet), campos:
             
             -row_key: Identificador unico de registro.
             -partition_key: Identificador unico de registro resumido.
             -nombre_app_origen: nombre de la BD / APP migrada.
             -nombre_tbl_sub_app_origen: Nombre de de la suaplicación en caso de que exista.
             -nombre_tbl_app_origen: Nombre de la tabla en BD Origen.
             -carpetaBlobTabla: Historicos_APP_BD/app-semih/metadatos.
             -copyTime: Estampa de tiempo de extraccion y copiado de la tabla de la BD hacia el blob en archivo formato .parquet.
             -duration: Tiempo de extraccion y copiado de la tabla de la BD hacia el blob en archivo formato .parquet.
             -rowsRead: Filas leidas de la tabla en la BD Origen.
             -filesWritten: archivos generados en la copia al blob Storage.
             -status: estados de la tarea de copiado Succeded, Filed, Procces.
             -throughput: tasa de trasfencia de la información origen IR, destino Blob.
             -carpetaOrigen: carpeta de la cual fue leida la metadata.
             -datosleidos: numero de bytes leidos.
             -datosescritos: número de bytes copiados, se comprime peso a un 90% para formato .parquet.
             -rowsCopied: número de filas copiadas.
             -errores: Lista de errores generados al momento de compiar la tabla en formato .parquet.
             -rutaDestinoTablaBlob: Ruta donde se copio la tabla en blob storage.
             -rutaDestinoTablaDataLake: Ruta donde se almacenara la tabla en el DL.
             -fecha_registro:Fecha en que se registro el log
               
Responsables: Juan David Escobar E.
Fecha Creación: 21/09/2020.
Fecha Modificación: 15/10/2020.
rowKey = llave primaria (nombre_app_origen + '-' + nombre_tbl_sub_app_origen + '-' + nombre_tbl_app_origen)
"""

def create_table_aud_blob():

  sql_db_aud =  "CREATE DATABASE IF NOT EXISTS auditoria"
  sql_drop =  "DROP TABLE IF EXISTS auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob"
  sql_tbl_aud = "CREATE TABLE auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob \
               ( \
                  rowKey string, \
                  partitionKey string,\
                  nombre_app_origen string, \
                  nombre_tbl_sub_app_origen string, \
                  nombre_tbl_app_origen string, \
                  carpetaBlobTabla string, \
                  copyTime string, \
                  duration string, \
                  rowsRead string, \
                  filesWritten string, \
                  status string, \
                  throughput string, \
                  carpetaOrigen string, \
                  datosleidos string, \
                  datosescritos string, \
                  rowsCopied string, \
                  errores string, \
                  rutaDestinoTablaBlob string, \
                  rutaDestinoTablaDataLake string, \
                  fecha_registro timestamp \
              ) \
              USING DELTA \
              PARTITIONED BY (nombre_app_origen) \
              LOCATION '/mnt/rdh-auditoria/results/Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob'";
  
  sqlContext.sql(sql_db_aud)
  sqlContext.sql(sql_drop)
  sqlContext.sql(sql_tbl_aud)

# COMMAND ----------

# DBTITLE 1,CREACION TABLA AUD (Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake - LOG AUD REGISTROS BD)
"""
Descripción: Log Carga Tablas DL: Se utiliza una tabla de log de auditoria para llevar el control y la trazabilidad de las tablas creadas en la zona de resultados, adicional
             almacena la información asociada a los datos que se insertaron, a continuación se describe cada uno de los campos de tabla:
             
             -row_key: Identificador unico de registro.
             -partition_key: Identificador unico de registro resumido.
             -nombre_app_origen: nombre de la BD / APP migrada.
             -nombre_tbl_sub_app_origen: Nombre de de la suaplicación en caso de que exista.
             -nombre_tbl_app_origen: Nombre de la tabla en BD Origen.
             -nombre_tbl_zdr: Nombre de la tabla en ZDR.
             -ruta_destino_tabla_data_lake: Ruta en la que se almacenan los datos de la tabla en el Datalake.
             -cantidad_registros_ZDC: Cantidad de registros leidos en un DF a partir de un archivo en formato .parquet en Blob storage.
             -cantidad_registros_ZDR: Cantidad de registros leidos en un DF a partir de la tabal creada en ZDR.
             -cantidad_md5_invalid: MD5 creado a partir de la concatencacion de campos de la tabla de ZDC vs ZDR, valida la integridad de los datos.
             -cantidad_duplicados: Cantidad de registros duplicados hallados en la tabla ZDR.
             -cantidad_corrupts: Cantidad de registros corruptos encontrados en la tabla ZDR.
             -cantidad_no_cargados: Resta de registros origen - registros cargados en tabla ZDR.
             -error_reg_zdc_zdr: Mensaje de error y detalle del campo cantidad_registros_ZDR.
             -error_reg_duplicados: Mensaje de error y detalle del campo cantidad_duplicados.
             -error_invalid_md5: Mensaje de error y detalle del campo cantidad_md5_invalid.
             -error_reg_corruptos: Mensaje de error y detalle del campo cantidad_corrupts.
             -duracion: Tiempo de procesamiento para crear la tabla en script 04_CreateDynamicDataModel.
             -fecha_registro: Fecha en que se registro el log.
             
Responsables: Juan David Escobar E.
Fecha Creación: 08/10/2020.
Fecha Modificación: 15/10/2020.
rowKey = llave primaria (nombre_app_origen + '-' + nombre_tbl_sub_app_origen + '-' + nombre_tbl_app_origen)
"""

def create_table_aud_bd():

  sql_db_aud =  "CREATE DATABASE IF NOT EXISTS auditoria"
  sql_drop =  "DROP TABLE IF EXISTS auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake"
  sql_tbl_aud = "CREATE TABLE auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake \
                 ( \
                    row_key string, \
                    partition_key string, \
                    nombre_app_origen string, \
                    nombre_tbl_sub_app_origen string, \
                    nombre_tbl_app_origen string, \
                    nombre_tbl_zdr string, \
                    ruta_destino_tabla_data_lake string, \
                    cantidad_registros_ZDC string, \
                    cantidad_registros_ZDR string, \
                    cantidad_md5_invalid string, \
                    cantidad_duplicados string, \
                    cantidad_corrupts string, \
                    cantidad_no_cargados string, \
                    error_reg_zdc_zdr string, \
                    error_reg_duplicados string, \
                    error_invalid_md5 string, \
                    error_reg_corruptos string, \
                    duracion string, \
                    fecha_registro timestamp \
                 ) \
                 USING DELTA \
                 PARTITIONED BY (nombre_app_origen) \
                 LOCATION '/mnt/rdh-auditoria/results/Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake'"
  
  sqlContext.sql(sql_db_aud)
  sqlContext.sql(sql_drop)
  sqlContext.sql(sql_tbl_aud)

# COMMAND ----------

def init_tbls_aud():
  create_table_aud_metadata()
  create_table_aud_blob()
  create_table_aud_bd()  

# COMMAND ----------

init_tbls_aud()