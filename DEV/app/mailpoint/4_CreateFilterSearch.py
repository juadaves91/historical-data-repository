# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from functools import reduce

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE mailpoint.zdr_mailpoint_src_metadatos_inventario_tablas_bd;
# MAGIC SELECT *
# MAGIC from mailpoint.zdr_mailpoint_src_metadatos_inventario_tablas_bd

# COMMAND ----------

results_path = '/mnt/app-mailpoint/results/SCHmailpoint/zdr_panel_'
name_table = '_FILTRO_BUSQUEDA'
query = 'select busqueda as filtro_busqueda from mailpoint.'
partitions = 20

sqlContext.sql('REFRESH TABLE mailpoint.zdr_mailpoint_src_metadatos_inventario_tablas_bd')
df_metadatos = sqlContext.sql("select NombreSubAplicacion, \
                                      translate(concat('ZDR_', NombreAplicacion, '_', 'panel', '_', NombreTablaAplicacion), ' -', '__') as tablas_result \
                                      from mailpoint.zdr_mailpoint_src_metadatos_inventario_tablas_bd")

# COMMAND ----------

"""
Descripción: Extrae de cada tabla una columna y los unifica en un list compression
Autor: Blaimir Ospina Cardona
Retorno: Un dataframe con las columnas unificadas
Fecha creción: 27/11/2020.
"""

def ft_append_campo_busqueda(prm_sub_app):
  
  df_busqueda_unificada = reduce(DataFrame.unionAll, [sqlContext.sql(query + tabla['tablas_result']) for tabla in df_metadatos.select('tablas_result'
                                                           ).filter(col('NombreSubAplicacion'
                                                           ) == prm_sub_app).collect()]) 
                                                           
  
  return df_busqueda_unificada.distinct()

# COMMAND ----------

"""
Descripción: Funcion para reemplazar espacios y guienes medios
Autor: Blaimir Ospina Cardona
Retorno: Una cadena sin espacion y guines medios
Fecha creción: 27/11/2020.
"""

def ft_trasnlate_string(prm_string):
  replace_aux = prm_string.maketrans(' -', '__')
  
  return prm_string.translate(replace_aux)

# COMMAND ----------

"""
Descripción: Recorre cada una de las aplicaciones y materializa las tablas asociadas
             por cada aplicacion unificando los campos de busqueda.
Autor: Blaimir Ospina Cardona
Retorno: --
Fecha creción: 27/11/2020.
"""

def ft_procesa_campos_busqueda():
  
  for sub_app in df_metadatos.select('NombreSubAplicacion').distinct().collect():
    
      try:
          print('zdr_mailpoint_' + ft_trasnlate_string(sub_app['NombreSubAplicacion']))
          sqlContext.sql('drop table if exists ' + 'zdr_mailpoint_panel' + ft_trasnlate_string(sub_app['NombreSubAplicacion']) + '_FILTRO_BUSQUEDA')
          
          df_busqueda_unificada_materializar = ft_append_campo_busqueda(sub_app['NombreSubAplicacion'])
          df_busqueda_unificada_materializar.repartition(partitions
                                                           ).write.format("delta"
                                                           ).mode("overwrite"
                                                           ).option('path', results_path + ft_trasnlate_string(sub_app['NombreSubAplicacion']) + '_FILTRO_BUSQUEDA'
                                                           ).saveAsTable('mailpoint.zdr_mailpoint_panel' + ft_trasnlate_string(sub_app['NombreSubAplicacion']) + '_FILTRO_BUSQUEDA')
          print('Campos Unificados correctamente para:', sub_app['NombreSubAplicacion'])'''
          
      except Exception as error:
         print('Error al Unificar los campos de busqueda para:', sub_app['NombreSubAplicacion'], '\n', str(error))

ft_procesa_campos_busqueda()
