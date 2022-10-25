# Databricks notebook source
from datetime import datetime
import time

while(True):
  print(datetime.now())
  time.sleep(60 * 5)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT TBL_BLOB.* 
# MAGIC FROM auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob AS TBL_BLOB 
# MAGIC LEFT JOIN auditoria.zdr_auditoria_log_extraction_bd_tbl_in_data_lake AS TBL_DL 
# MAGIC ON TBL_BLOB.rowKey = TBL_DL.row_key 
# MAGIC WHERE TBL_DL.row_key IS NULL 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM auditoria.zdr_auditoria_log_extraction_bd_tbl_in_blob AS BLOB 
# MAGIC   LEFT JOIN SEMIH.zdr_semih_src_metadatos_inventario_tablas_bd  AS INV
# MAGIC     ON BLOB.nombre_tbl_app_origen =  INV.NombreTablaAplicacion 
# MAGIC     WHERE rowKey IS NULL
# MAGIC     
# MAGIC 
# MAGIC --SEMIH NOMUS_APP | SEMIH - NOMUS_APP | SEMIHPDB | SCHSEMIH | ORACLE | TMVACU00
# MAGIC 
# MAGIC -- '\nlist_result_validations_metadata = "[{"dic_result_metadata_cout_rows":{"is_valid":true,"msg_error":""}},{"dic_result_metadata_valid":{"is_valid":true,"msg_error":""}},{"dic_result_metadata_count_rows_current_tbl":{"is_valid":false,"msg_error":"La cantidad de registros de la tabla: HISDATOSEMPLEO no coinciden, CantidadRegistros Formato_ArchivosFuente_Generico.xlsx: 99999, cantidad registros hallados en la BD: 32533"}}]"\n\nnombre_app_origen = SEMI-H\nnombre_bd_origen = SEMIHDDB\nnombre_esquema_tbl_origen = SCHSEMIH\nnombre_tbl_app_origen = HISACUMANOCONCEPTO\nnombre_tbl_sub_app_origen = PEOPLESOFT\nnombre_tipo_bd_origen = ORACLE\n'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from auditoria.zdr_auditoria_log_extraction_bd_tbl_in_blob WHERE  
# MAGIC nombre_tbl_app_origen = 'HISACUMANOCONCEPTO'

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE SEMIH.zdr_semih_src_metadatos_inventario_tablas_bd;
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM SEMIH.zdr_semih_src_metadatos_inventario_tablas_bd  AS INV
# MAGIC   LEFT JOIN auditoria.zdr_auditoria_log_extraction_bd_tbl_in_blob AS BLOB
# MAGIC     ON INV.NombreTablaAplicacion = BLOB.nombre_tbl_app_origen
# MAGIC WHERE  BLOB.nombre_tbl_app_origen IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE SEMIH.zdr_semih_src_metadatos_inventario_tablas_bd;
# MAGIC SELECT COUNT(*) FROM SEMIH.zdr_semih_src_metadatos_inventario_tablas_bd   

# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from auditoria.zdr_auditoria_log_extraction_bd_tbl_in_blob 

# COMMAND ----------

print(9*150000)


# COMMAND ----------

# MAGIC %sql
# MAGIC select DISTINCT PAGINADO FROM SEMIH.zdr_SEMIH_PORFIN_PFICO995

# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM auditoria.zdr_auditoria_log_extraction_bd_tbl_in_data_lake
# MAGIC --where nombre_tbl_sub_app_origen like '%PORFIN%'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC 
# MAGIC FROM SEMIH.zdr_semih_src_metadatos_inventario_tablas_bd   AS TBL_INV_2 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT   TBL_INV_2.NombreBaseDeDatos,
# MAGIC 		 TBL_INV_2.NombreAplicacion,
# MAGIC 		 TBL_INV_2.NombreSubAplicacion,
# MAGIC 		 TBL_INV_2.NombreTablaAplicacion,
# MAGIC 		 TBL_INV_2.Esquema,
# MAGIC 		 TBL_INV_2.TipoBD,
# MAGIC 		 TBL_INV_2.Query,
# MAGIC 		 TBL_INV_2.QueryCount,
# MAGIC 		 TBL_INV_2.CantidadColumnas, 
# MAGIC 		 TBL_INV_2.CantidadRegistros
# MAGIC  FROM  SEMIH.zdr_semih_src_metadatos_inventario_tablas_bd   AS TBL_INV_2 
# MAGIC  LEFT JOIN auditoria.zdr_auditoria_log_extraction_bd_tbl_in_data_lake AS TBL_DL  
# MAGIC    ON TBL_INV_2.row_key = TBL_DL.row_key  
# MAGIC  WHERE TBL_DL.row_key IS NULL
# MAGIC  ORDER BY  NombreSubAplicacion, NombreTablaAplicacion DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC  FROM 
# MAGIC  (SELECT NombreBaseDeDatos, 
# MAGIC 		 NombreAplicacion, 
# MAGIC 		 NombreSubAplicacion, 
# MAGIC 		 NombreTablaAplicacion, 
# MAGIC 		 Esquema, 
# MAGIC 		 TipoBD, 
# MAGIC 		 Query, 
# MAGIC 		 QueryCount, 
# MAGIC 		 CantidadColumnas, 
# MAGIC 		 CantidadRegistros       
# MAGIC  FROM   SEMIH.zdr_semih_src_metadatos_inventario_tablas_bd   AS TBL_INV_1
# MAGIC  WHERE RequiereReprocesarMetadatos = "Si"
# MAGIC  UNION 
# MAGIC  SELECT  NombreBaseDeDatos,
# MAGIC 		 NombreAplicacion,
# MAGIC 		 NombreSubAplicacion,
# MAGIC 		 NombreTablaAplicacion,
# MAGIC 		 Esquema,
# MAGIC 		 TipoBD,
# MAGIC 		 Query,
# MAGIC 		 QueryCount,
# MAGIC 		 CantidadColumnas, 
# MAGIC 		 CantidadRegistros,
# MAGIC  FROM  SEMIH.zdr_semih_src_metadatos_inventario_tablas_bd   AS TBL_INV_2 
# MAGIC  LEFT JOIN auditoria.zdr_auditoria_log_extraction_bd_tbl_in_data_lake AS TBL_DL  
# MAGIC    ON TBL_INV.row_key = TBL_DL.row_key  
# MAGIC  WHERE TBL_DL.row_key IS NULL
# MAGIC  ORDER BY  NombreSubAplicacion, NombreTablaAplicacion DESC)  TEMP_TBL_INV

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- UPDATE auditoria.zdr_auditoria_log_extraction_bd_tbl_in_blob SET nombre_tbl_sub_app_origen = 'INCENTIVOS_PLUS'
# MAGIC -- UPDATE auditoria.zdr_auditoria_log_extraction_bd_tbl_in_blob SET partitionKey = CONCAT(nombre_tbl_sub_app_origen, '-', nombre_tbl_app_origen)
# MAGIC -- UPDATE auditoria.zdr_auditoria_log_extraction_bd_tbl_in_blob SET rowKey = CONCAT(nombre_app_origen, '-' ,nombre_tbl_sub_app_origen, '-', nombre_tbl_app_origen)
# MAGIC -- WHERE nombre_tbl_sub_app_origen = 'INCENTIVOS_PLUS';
# MAGIC 
# MAGIC 
# MAGIC SELECT * FROM auditoria.zdr_auditoria_log_extraction_bd_tbl_in_blob
# MAGIC WHERE nombre_tbl_sub_app_origen = 'SIF_COMICOL';
# MAGIC 
# MAGIC 
# MAGIC --|APP = SEMIH | SUBAPP = AFC | ROWKEY = SEMIH-AFC-DXM_USUARIOS
# MAGIC 
# MAGIC -- 'AFC'                   --OK
# MAGIC -- 'CREDITO_HIPOTECARIO';  --OK
# MAGIC -- 'INCENTIVOS_PLUS'       --OK
# MAGIC -- 'NOMUS_APP'             --OK
# MAGIC -- 'NOMUS_VALORES'         --OK
# MAGIC -- 'PEOPLE_APP'            --OK
# MAGIC -- 'PLENITUD'              --OK
# MAGIC -- 'PORFIN'                --OK
# MAGIC -- 'PRUEBA'                --OK
# MAGIC -- 'SICCO'                 --OK
# MAGIC -- 'SIF_COMICOL'           --OK

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM auditoria.zdr_auditoria_log_extraction_bd_tbl_in_data_lake
# MAGIC WHERE nombre_tbl_sub_app_origen LIKE '%CREDITO HIPOTECARIO%';
# MAGIC 
# MAGIC --|APP = SEMIH | SUBAPP = AFC | ROWKEY = SEMIH-AFC-DXM_USUARIOS
# MAGIC --|APP = SEMIH | SUBAPP = AFC | ROWKEY = SEMIH-AFC-DXM_USUARIOS
# MAGIC 
# MAGIC -- 'AFC'                   --OK
# MAGIC -- 'CREDITO HIPOTECARIO';  --PENDIENTE CARGAR
# MAGIC -- 'INCENTIVOS PLUS'       --PENDIENTE CARGAR
# MAGIC -- 'NOMUS_APP'             --OK
# MAGIC -- 'NOMUS_VALORES'         --PENDIENTE CARGAR
# MAGIC -- 'PEOPLE_APP'            --OK
# MAGIC -- 'PLENITUD'              --OK
# MAGIC -- 'PORFIN'                --OK
# MAGIC -- 'PRUEBA'                --PENDIENTE CARGAR
# MAGIC -- 'SICCO'                 --PENDIENTE CARGAR
# MAGIC -- 'SIF_COMICOL'           --PENDIENTE CARGAR

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --UPDATE auditoria.zdr_auditoria_log_extraction_bd_validations_metadata SET nombre_sub_app_origen = 'NOMUS_VALORES'
# MAGIC --UPDATE auditoria.zdr_auditoria_log_extraction_bd_validations_metadata SET partitionKey = CONCAT(nombre_sub_app_origen, '-', nombre_tbl_app_origen)
# MAGIC -- UPDATE auditoria.zdr_auditoria_log_extraction_bd_validations_metadata SET rowKey = CONCAT(nombre_app_origen, '-' ,nombre_sub_app_origen, '-', nombre_tbl_app_origen)
# MAGIC -- WHERE nombre_sub_app_origen = 'NOMUS_VALORES';
# MAGIC 
# MAGIC SELECT * FROM auditoria.zdr_auditoria_log_extraction_bd_validations_metadata
# MAGIC WHERE nombre_sub_app_origen LIKE '%NOMUS_VALORES%';
# MAGIC 
# MAGIC -- |APP = SEMIH | SUBAPP = AFC | ROWKEY = SEMIH-AFC-DXM_USUARIOS
# MAGIC -- |APP = SEMIH | SUBAPP = AFC | ROWKEY = SEMIH-AFC-DXM_USUARIOS
# MAGIC -- |APP = SEMIH | SUBAPP = AFC | ROWKEY = SEMIH-AFC-DXM_USUARIOS
# MAGIC 
# MAGIC -- 'AFC'                   --OK
# MAGIC -- 'CREDITO HIPOTECARIO';  --OK
# MAGIC -- 'INCENTIVOS PLUS'       --PENDIENTE REGISTRO SIN ERRORES DE METADATA
# MAGIC -- 'NOMUS_APP'             --OK
# MAGIC -- 'NOMUS_VALORES'         --OK
# MAGIC -- 'PEOPLE_APP'            --PENDIENTE REGISTRO SIN ERRORES DE METADATA
# MAGIC -- 'PLENITUD'              --PENDIENTE REGISTRO SIN ERRORES DE METADATA
# MAGIC -- 'PORFIN'                --OK
# MAGIC -- 'PRUEBA'                --PENDIENTE REGISTRO SIN ERRORES DE METADATA
# MAGIC -- 'SICCO'                 --PENDIENTE REGISTRO SIN ERRORES DE METADATA
# MAGIC -- 'SIF_COMICOL'           --PENDIENTE REGISTRO SIN ERRORES DE METADATA

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from semih.zdr_semih_src_metadatos_inventario_tablas_bd  AS TBL_INV_1;
# MAGIC NombreTablaAplicacion = 
# MAGIC 
# MAGIC 
# MAGIC @concat(' SELECT count(*) as count_rows ',
# MAGIC         ' FROM ', activity('Get_Output_Params_4_Create_Metadata_Tables').output.runOutput.tbl_inventarios_tbls_bd, 
# MAGIC         ' WHERE NombreTablaAplicacion =, '"',  item().NombreTablaAplicacion, '"',  
# MAGIC         ' AND RequiereReprocesarExtraccion = "Si" ')

# COMMAND ----------

activity('Get_Output_Params_4_Create_Metadata_Tables').output.runOutput.tbl_inventarios_tbls_bd,  
 '  AS TBL_INV 

# COMMAND ----------

SELECT *
FROM
(SELECT NombreBaseDeDatos,
		NombreAplicacion, 
		NombreSubAplicacion, 
		NombreTablaAplicacion,
		Esquema, 
		TipoBD,
		Query, 
		QueryCount,
		CantidadColumnas, 
		CantidadRegistros       
FROM semih.zdr_semih_src_metadatos_inventario_tablas_bd  AS TBL_INV_1 
WHERE RequiereReprocesar = 'Si'
UNION 
SELECT  NombreBaseDeDatos,
		NombreAplicacion, 
		NombreSubAplicacion, 
		NombreTablaAplicacion,
		Esquema, 
		TipoBD,
		Query, 
		QueryCount,
		CantidadColumnas, 
		CantidadRegistros
FROM semih.zdr_semih_src_metadatos_inventario_tablas_bd  AS TBL_INV 
LEFT JOIN auditoria.zdr_auditoria_log_extraction_bd_tbl_in_data_lake AS TBL_DL  
ON TBL_INV.row_key = TBL_DL.row_key  
WHERE TBL_DL.row_key IS NULL
ORDER BY  NombreSubAplicacion, NombreTablaAplicacion DESC
)  TEMP_TBL_INV

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC @concat(' SELECT * ',
# MAGIC ' FROM ',
# MAGIC ' (SELECT NombreBaseDeDatos, ',
# MAGIC 		' NombreAplicacion, ',
# MAGIC 		' NombreSubAplicacion, ',
# MAGIC 		' NombreTablaAplicacion, ',
# MAGIC 		' Esquema, ',
# MAGIC 		' TipoBD, ',
# MAGIC 		' Query, ',
# MAGIC 		' QueryCount, ',
# MAGIC 		' CantidadColumnas, ',
# MAGIC 		' CantidadRegistros ',       
# MAGIC ' FROM ' , activity('Get_Output_Params_4_Create_Metadata_Tables').output.runOutput.tbl_inventarios_tbls_bd,  ' AS TBL_INV_1 ',
# MAGIC ' WHERE RequiereReprocesar = "Si" ',
# MAGIC ' UNION ',
# MAGIC ' SELECT  NombreBaseDeDatos, ',
# MAGIC 		' NombreAplicacion, ',
# MAGIC 		' NombreSubAplicacion, ',
# MAGIC 		' NombreTablaAplicacion, ',
# MAGIC 		' Esquema, ',
# MAGIC 		' TipoBD, ',
# MAGIC 		' Query, ',
# MAGIC 		' QueryCount, ',
# MAGIC 		' CantidadColumnas, ',
# MAGIC 		' CantidadRegistros ',
# MAGIC ' FROM ' , activity('Get_Output_Params_4_Create_Metadata_Tables').output.runOutput.tbl_inventarios_tbls_bd,  ' AS TBL_INV_2 ',
# MAGIC ' LEFT JOIN auditoria.zdr_auditoria_log_extraction_bd_tbl_in_data_lake AS TBL_DL  ',
# MAGIC ' ON TBL_INV.row_key = TBL_DL.row_key  ',
# MAGIC ' WHERE TBL_DL.row_key IS NULL ',
# MAGIC ' ORDER BY  NombreSubAplicacion, NombreTablaAplicacion DESC ',
# MAGIC ' )  TEMP_TBL_INV ')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from semih.zdr_semih_nomus_app_bahisacumanoconcepto;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM auditoria.zdr_auditoria_log_extraction_bd_tbl_in_data_lake;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM semih.zdr_semih_src_metadatos_inventario_tablas_bd

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  * FROM auditoria.zdr_auditoria_log_extraction_bd_validations_metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *  FROM auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) as count_rows FROM auditoria.zdr_auditoria_log_extraction_bd_tbl_in_data_lake

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls /dbfs/mnt/app-semih/results/
# MAGIC #rm -rf /dbfs/mnt/rdh-auditoria/results/Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake
# MAGIC #rm -rf /dbfs/mnt/rdh-auditoria/results/Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata

# COMMAND ----------

zdr_auditoria_log_extraction_bd_tbl_in_blob
zdr_auditoria_log_extraction_bd_tbl_in_data_lake
zdr_auditoria_log_extraction_bd_validations_metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM auditoria.zdr_auditoria_log_extraction_bd_tbl_in_data_lake TBL_DL

# COMMAND ----------

@concat('SELECT NombreBaseDeDatos, NombreAplicacion, NombreSubAplicacion, NombreTablaAplicacion, Esquema, TipoBD, Query, QueryCount, CantidadColumnas, CantidadRegistros FROM ', activity('CreacionTablasMetadata').output.runOutput.tbl_inventarios_tbls_bd)

# COMMAND ----------

@concat('SELECT NombreBaseDeDatos, NombreAplicacion, NombreSubAplicacion, NombreTablaAplicacion, Esquema, TipoBD, Query, QueryCount, CantidadColumnas, CantidadRegistros FROM ', activity('CreacionTablasMetadata').output.runOutput.tbl_inventarios_tbls_bd,  'AS TBL_INV', 'LEFT OUTER JOIN auditoria.zdr_auditoria_log_extraction_bd_tbl_in_data_lake TBL_DL', 'ON TBL_INV.row_key = TBL_DL.row_key',       'WHERE TBL_DL.row_key IS NULL')

# COMMAND ----------

'SELECT * FROM auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob WHERE status = ' + "'" + status  + "'" + ' and nombre_app_origen = '  + "'" + application  + "'" 

# COMMAND ----------

@concat(item().Esquema, '_',  item().NombreTablaAplicacion)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC @concat("SELECT count(*) as count_rows FROM auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob", "WHERE status = ", "'Succeeded'", " and ", "rowKey = ", "'" , item().NombreAplicacion,'-' ,item().NombreSubAplicacion, '-' ,item().NombreTablaAplicacion, "'")
# MAGIC -- WHERE status = "'" + status  + "'" AND nombre_tbl_app_origen = + "'" + current_tbl  + "'" +

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT TBL_BLOB.* 
# MAGIC FROM auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob AS TBL_BLOB
# MAGIC LEFT JOIN auditoria.zdr_auditoria_log_extraction_bd_tbl_in_data_lake AS TBL_DL
# MAGIC  ON TBL_BLOB.rowKey = TBL_DL.row_key
# MAGIC  WHERE TBL_DL.row_key IS NULL 
# MAGIC        AND TBL_BLOB.status = 'Succeeded' 
# MAGIC        and TBL_BLOB.nombre_app_origen = 'SEMIH'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --1. Tablas del inventario que ya estan cargadas en el DataLake no se obtienen.
# MAGIC SELECT *
# MAGIC FROM SEMIH.Zdr_semih_Src_Metadatos_Inventario_Tablas_BD TBL_INV 
# MAGIC LEFT JOIN auditoria.zdr_auditoria_log_extraction_bd_tbl_in_data_lake TBL_DL
# MAGIC   ON TBL_INV.row_key = TBL_DL.row_key
# MAGIC WHERE TBL_DL.row_key IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM SEMIH.Zdr_semih_Src_Metadatos_Inventario_Tablas_BD TBL_INV
# MAGIC INNER JOIN  auditoria.zdr_auditoria_log_extraction_bd_validations_metadata TBL_METADATOS
# MAGIC   ON TBL_INV.row_key = TBL_METADATOS.RowKey
# MAGIC INNER JOIN  auditoria.zdr_auditoria_log_extraction_bd_tbl_in_blob TBL_BLOB
# MAGIC   ON TBL_INV.row_key = TBL_BLOB.rowKey
# MAGIC INNER JOIN auditoria.zdr_auditoria_log_extraction_bd_tbl_in_data_lake TBL_DL
# MAGIC   ON TBL_INV.row_key = TBL_DL.row_key
# MAGIC   
# MAGIC --INVENTARIO
# MAGIC -- SELECT *
# MAGIC -- FROM SEMIH.Zdr_semih_Src_Metadatos_Inventario_Tablas_BD TBL_INV
# MAGIC 
# MAGIC -- LOG VALIDACION METADATOS
# MAGIC -- SELECT * 
# MAGIC -- FROM auditoria.zdr_auditoria_log_extraction_bd_validations_metadata TBL_METADATOS
# MAGIC 
# MAGIC -- LOG TBL IN BLOB
# MAGIC -- SELECT * 
# MAGIC -- FROM auditoria.zdr_auditoria_log_extraction_bd_tbl_in_blob TBL_BLOB
# MAGIC 
# MAGIC -- LOGTBL IN DATA LAKE
# MAGIC -- SELECT *
# MAGIC -- FROM auditoria.zdr_auditoria_log_extraction_bd_tbl_in_data_lake TBL_DL