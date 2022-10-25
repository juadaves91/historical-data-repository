# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC # CONCATENA VALORES PARA GENRAR QUERY DE EXTRACCION DE METATADOS EN LA BD POR CADA TABLA

# COMMAND ----------

# DBTITLE 1,Mapeo de variables
"""Path Ruta de la Aplicacion"""
# Ejemplo ejecucion: 

dbutils.widgets.text("query_metadata_current_tbl", "","")
dbutils.widgets.get("query_metadata_current_tbl")
query_metadata_current_tbl = getArgument("query_metadata_current_tbl")

dbutils.widgets.text("schema_current_tbl", "","")
dbutils.widgets.get("schema_current_tbl")
schema_current_tbl = getArgument("schema_current_tbl")

dbutils.widgets.text("name_current_tbl", "","")
dbutils.widgets.get("name_current_tbl")
name_current_tbl = getArgument("name_current_tbl")

# COMMAND ----------

print(query_metadata_current_tbl)

# COMMAND ----------

#Replace Schema and CurrentTable
query_metadata_current_tbl_fomated = query_metadata_current_tbl.replace("[item().Esquema]", "'" + schema_current_tbl + "'").replace("[item().NombreTablaAplicacion]", "'" +  name_current_tbl + "'")
print(query_metadata_current_tbl_fomated)


# COMMAND ----------

"""
Descripción: Retorna valor deL QUERY para consultar los metadatos de la tabla actual
"""

import json

dbutils.notebook.exit(json.dumps({
  "query_metadata_current_tbl": query_metadata_current_tbl_fomated
}))