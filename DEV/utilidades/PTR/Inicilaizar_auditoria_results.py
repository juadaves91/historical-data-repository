# Databricks notebook source
import os

"""Variables """
path = '/dbfs/mnt/rdh-auditoria/results/'
data_base = 'auditoria'

# COMMAND ----------

sqlContext.sql('drop database if exists ' + data_base + ' cascade')

# COMMAND ----------

def ft_materializar_tablas_auditoria(path, name_table):
  sqlContext.sql("create database if not exists " + data_base)
  sqlContext.sql("drop table if exists " + data_base + "." + name_table)
  sqlContext.createExternalTable(data_base + "." + name_table,path.replace('/dbfs',''))

# COMMAND ----------

error = ''
for tables in os.listdir(path):
  try:
    if 'ZDR' in tables.upper():
      ft_materializar_tablas_auditoria(path + tables, tables.lower()) 
    else:
      ft_materializar_tablas_auditoria(path + tables, 'zdr_' + tables.lower()) 
    print('tabla de auditoria', tables, 'creada')
  except Exception as error:
    print('Error construyendo la tabla', tables , 'en la ruta:', path + tables, error)
