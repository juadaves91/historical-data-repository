# Databricks notebook source
# MAGIC %md
# MAGIC ## Función Borrado de Objeto Tipo Vista o Tabla: 
# MAGIC Mediante un Try y Except realiza la eliminación de una tabla o vista de acuerdo a lo que se encuentre registrado en el metastore de Hive

# COMMAND ----------

def fn_view_or_table(ar_eschema, ar_table):  
  try:       
    spark.sql('DROP VIEW IF EXISTS ' + ar_eschema + '.' + ar_table)      
  except:
    spark.sql('DROP TABLE IF EXISTS ' + ar_eschema + '.' + ar_table) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Función Borrado de carpetas y archivos en Data Lake: 
# MAGIC Mediante un Try y Except realiza la eliminación de archivos de una ruta en el Data Lake

# COMMAND ----------

def fn_delete_folder_full_name(carpeta):
  try:
    path = "/mnt/"+carpeta
    carpetas=dbutils.fs.ls(path)    
    registros = len(carpetas)
    if registros > 0:      
      for i in range(registros):       
        dbutils.fs.rm(path + carpetas[i].name, recurse=True)
  except Exception as error:
    print(error)   

# COMMAND ----------

# MAGIC %md
# MAGIC ## Función Borrado de carpetas y archivos en Data Lake: 
# MAGIC Mediante un Try y Except realiza la eliminación de archivos de la ruta results en el Data Lake

# COMMAND ----------

def fn_delete_folder(carpeta):
  try:
    path = "/mnt/"+carpeta+"/results/"
    carpetas=dbutils.fs.ls(path)    
    registros = len(carpetas)
    if registros > 0:      
      for i in range(registros):       
        dbutils.fs.rm(path + carpetas[i].name, recurse=True)
  except Exception as error:
    print(error)    