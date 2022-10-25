# Databricks notebook source
# DBTITLE 1,Mapeo de variables
"""Path Ruta de la Aplicacion"""
# Ejemplo ejecucion: folder_blob = app-semih/metadatos
dbutils.widgets.text("folder_blob", "","")
dbutils.widgets.get("folder_blob")
folder_blob = getArgument("folder_blob")

application = folder_blob.split('/')[0]
sub_folder_blob = folder_blob.split('/')[1]
application_db = application.split('-')[1]

# COMMAND ----------

"""
Obtiene los valores almacenados como salida en el pipeline 4-CreateTablesMetadata
"""

def get_output_vars_tbl_aux():
  prm_aux_tbl = 'PARAM_' + application_db.upper() + '_APP_HELP_PIPELINE_CREATE_TABLE'

  tbl_aux_output_pipeline_create_metadata = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='" + prm_aux_tbl + "' LIMIT 1").first()["Valor1"]

  sqlContext.sql('REFRESH TABLE ' + application_db + '.' + tbl_aux_output_pipeline_create_metadata)
  df = sqlContext.sql('SELECT * FROM ' + application_db + '.' + tbl_aux_output_pipeline_create_metadata)
  return df

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC ### RETORNO - OUTPUT NOTEBOOK

# COMMAND ----------

"""
Descripción: Retorna valor de tablas creadas con la información de los metadatos
"""

import json

df = get_output_vars_tbl_aux()

dbutils.notebook.exit(json.dumps({
  "tbl_prm_metadatos": df.first()["tbl_prm_metadatos"],
  "tbl_metadatos": df.first()["tbl_metadatos"],
  "tbl_inventarios_tbls_bd": df.first()["tbl_inventarios_tbls_bd"],
  "tipo_bd_origen": df.first()["tipo_bd_origen"],
  "application": df.first()["application"],
  "sub_application": df.first()["sub_application"], 
  "dominio_actual": df.first()["dominio_actual"] 
}))