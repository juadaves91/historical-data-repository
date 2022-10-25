# Databricks notebook source
# MAGIC %md
# MAGIC #Auditoria Registros BD

# COMMAND ----------

if origen == "":
  arr_files_str = ",".join(arr_files)
  try:
    dbutils.notebook.run("/dbbicorporativo/dbbicorporativo-indicadores/8_Execute_Auditoria", 72000, {"arr_files": str(arr_files_str), "application": str(application)})
  except Exception as error:
    print(error, "\n Error al ejectuar la auditoria")

# COMMAND ----------

if origen != "":
  dbutils.widgets.text("file_name", "","")
  dbutils.widgets.get("file_name")
  file_name = getArgument("file_name")

  dbutils.widgets.text("error", "","")
  dbutils.widgets.get("error")
  error_json = getArgument("error")

#   application = "dbbicorporativo-indicadores"
  
  zone = pytz.timezone('America/Bogota')
  date = datetime.now(zone)
  data_base = "auditoria"
  table_name = "zdr_lotus_auditoria_json"
  path_auditoria = '/mnt/utilidades/panel_auditoria'

# COMMAND ----------

if origen != "":
  df_registros = indicadores_df
  panel_principal = panel_principal_indicadores
  panel_detalle = panel_detalle_indicadores
  
  registros_tabla_intermedia = df_registros
  registros_tabla_principal = panel_principal.count()
  registros_tabla_detalle = panel_detalle.select(col("IdRegistro")).distinct().count()

# COMMAND ----------

# MAGIC %run /dbbicorporativo/dbbicorporativo-indicadores/Helper_Functions_Auditoria_Registros

# COMMAND ----------

if origen != "":
  columns_duplicates = ['JSON', 'CARPETA']
  ft_precesar("IdRegistro", columns_duplicates)
