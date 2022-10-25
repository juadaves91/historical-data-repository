# Databricks notebook source
# DBTITLE 1,PARAMETROS DE ENTRADA
dbutils.widgets.text("nombre_app", "","")
dbutils.widgets.get("nombre_app")
NombreAplicacion = getArgument("nombre_app")

dbutils.widgets.text("nombre_sub_app", "","")
dbutils.widgets.get("nombre_sub_app")
NombreSubAplicacion = getArgument("nombre_sub_app")

# COMMAND ----------

"""
-Descripción: Obtiene una lista de los tipos de datos asociados a la tabla parámetrizada. 
-Autor: Juan Escobar.
-Fecha: 22/01/2021.
-Parametros: @NombreAplicacion = 'SEMIH' (*), 
             @NombreSubAplicacion = 'AFC' (*),
             @NombreTablaAplicacion = 'BADETALLEINCAPACIDAD' (*).
"""

def get_list_types(NombreAplicacion, NombreSubAplicacion, NombreTablaAplicacion, CampoIndentificadorBusqueda):
  
  refresh_list = ["REFRESH TABLE semih.zdr_semih_src_metadatos", "REFRESH TABLE semih.zdr_semih_src_metadatos_inventario_tablas_bd"]
  
  for x in refresh_list:
    sqlContext.sql(x)
    
  list_busqueda = CampoIndentificadorBusqueda.strip().split(",")
  list_busqueda.sort()
  format_prm_in_sql = ''
  for x in list_busqueda:
    format_prm_in_sql = format_prm_in_sql + "'" + x + "'" + "," 

  format_prm_in_sql = format_prm_in_sql[0 : len(format_prm_in_sql) - 1]
    
  query = "SELECT MET.TipoDatoHive tipo, \
                  MET.NombreCampoFuncional campo, \
                  INV.NombreTablaAplicacion tabla, \
                  INV.NombreAplicacion nombre_app, \
                  INV.NombreSubAplicacion nombre_sub_app\
          FROM semih.zdr_semih_src_metadatos_inventario_tablas_bd INV \
            INNER JOIN semih.zdr_semih_src_metadatos MET \
              ON INV.NombreAplicacion = MET.OrigenAplicacion \
                AND INV.NombreSubAplicacion = MET.OrigenSubAplicacion \
                AND INV.NombreTablaAplicacion = MET.TablaOrigen \
          WHERE INV.NombreAplicacion LIKE '%{0}%' AND   \
                INV.NombreSubAplicacion LIKE '%{1}%' AND \
                INV.NombreTablaAplicacion LIKE '%{2}%' AND \
                MET.NombreCampoFuncional  IN ({3}) \
          ORDER BY MET.TablaOrigen ".format(NombreAplicacion, NombreSubAplicacion, NombreTablaAplicacion, format_prm_in_sql)
  
  return sqlContext.sql(query), list_busqueda

get_list_types('SEMIH', 'AFC', 'DXM_LOG', 'LOG_DESCRIPCION,LOG_DONDE,LOG_QUIEN,LOG_FECHA')

# COMMAND ----------

"""
-Descripción: Obtiene una lista de los tipos de tablas asociados a la app y subApp parámetrizadas. 
-Autor: Juan Escobar.
-Fecha: 22/01/2021.
-Parametros: @NombreAplicacion = 'SEMIH' (*), 
             @NombreSubAplicacion = 'AFC' (*),
             @NombreTablaAplicacion = 'BADETALLEINCAPACIDAD (*)'.
"""

def get_list_tables_by_app(NombreAplicacion, NombreSubAplicacion):
  
  query = "SELECT NombreTablaAplicacion tabla, \
                  CampoIndentificadorBusqueda campos_busqueda, \
                  CampoFechaBusqueda fecha_busqueda, \
                  NombreAplicacion nombre_app, \
                  NombreSubAplicacion nombre_sub_app \
          FROM semih.zdr_semih_src_metadatos_inventario_tablas_bd  \
          WHERE NombreAplicacion LIKE '%{0}%' AND   \
                NombreSubAplicacion LIKE '%{1}%' \
          ORDER BY NombreTablaAplicacion ".format(NombreAplicacion, NombreSubAplicacion)
   
  return sqlContext.sql(query)
    
display(get_list_tables_by_app('SEMIH','AFC'))  

# COMMAND ----------

"""
-Descripción: Obtiene una lista de tablas y la información asociada de acuerdo a la app y subApp parámetrizadas. 
-Autor: Juan Escobar.
-Fecha: 22/01/2021.
-Parametros: @NombreAplicacion = 'SEMIH' (*), 
             @NombreSubAplicacion = 'AFC' (*),
-Retorno: [
            {
              "tabla": "DXM_LOG",
              "campos_busqueda" : ["LOG_DESCRIPCION", "LOG_DONDE", "LOG_QUIEN", "LOG_FECHA"],
              "tipos" : ["STRING", "STRING", "STRING", "DATE"],
              "fecha_busqueda": "LOG_FECHA_MIG",
              "nombre_app": 'AFC'
            }
          ]  
"""
def get_info_tables(NombreAplicacion, NombreSubAplicacion):
  ls_info_tablas = []
  df_info_tables = get_list_tables_by_app('SEMIH','AFC')
  df_info_tables = df_info_tables.rdd.toLocalIterator()
  
  tabla, campos_busqueda, tipos, fecha_busqueda, nombre_app , nombre_sub_app = '', '', '', '', '', ''

  dic_current_row = { 
                      "tabla" : "",
                      "campos_busqueda" : "",
                      "fecha_busqueda" : "",
                      "nombre_app" : "",
                      "nombre_sub_app" : "",
                      "tipos" : []
                     }
  
  for row in df_info_tables:
  
    dic_current_row = row.asDict()
    df_current_types, list_campos_busqueda = get_list_types(row['nombre_app'], row['nombre_sub_app'], row['tabla'], row['campos_busqueda'])
    df_current_types = df_current_types.rdd.collect()
        
    tipos = ''
    for row in df_current_types:
      tipos = tipos + "'" + row['tipo'] + "'" + ","

    tipos = tipos[0 : len(tipos) - 1]
    dic_current_row['tipos'] = tipos.split(',')
    dic_current_row['campos_busqueda'] = list_campos_busqueda
    ls_info_tablas.append(dic_current_row) 
     
  return ls_info_tablas

# COMMAND ----------

def get_uses_cases_by_app(NombreAplicacion, NombreSubAplicacion):
  
  from os import path, makedirs
  
  ls_info_tablas = get_info_tables(NombreAplicacion, NombreSubAplicacion) 
  bd_name = NombreAplicacion
  tbl_name = 'zdr_' + bd_name + '_'

  try:

    for x in ls_info_tablas:

        current_tbl = bd_name + '.' + tbl_name + NombreSubAplicacion + '_' + x['tabla']
        current_list_fields_sc = x['campos_busqueda']
        current_list_fields_type = x['tipos']
        current_field_date = x['fecha_busqueda']

        for j in range(0, len(current_list_fields_sc)):

            type_fl =  current_list_fields_type[j]
            field = current_list_fields_sc[j]

            cast_short_date = 'date_Format(' + field + ', "yyyy-MM-dd")'  if type_fl.upper() == "DATE" or type_fl.upper() == "TIMESTAMP" else field

            query = 'SELECT count(1) as CANTIDAD, \
                     {} as busqueda \
                     FROM {} \
                     GROUP BY {}, {} \
                     ORDER BY CANTIDAD DESC'.format(cast_short_date, current_tbl, field, current_field_date)

            df = sqlContext.sql(query)
            if type_fl.upper() == "DATE" or type_fl.upper() == "TIMESTAMP":       
              df = df.groupBy('busqueda').sum('CANTIDAD')
              
            df = df.toPandas()
            df = df.sample(10)
            
            current_path_uses_cs = "/dbfs/mnt/app-" + NombreAplicacion  + "-storage/casodeusos/" + NombreSubAplicacion
            current_path_uses_cs = current_path_uses_cs.lower()
            
            if (not path.exists(current_path_uses_cs + '/')):
              makedirs(current_path_uses_cs)              
            
            print(current_path_uses_cs + '/{}_{}.csv'.format(x['tabla'], field))
            df.to_csv(current_path_uses_cs + '/{}_{}.csv'.format(x['tabla'], field), index=False, sep='|')

  except Exception as error:
    print("no se proceso la tabla {}_{}".format(x['tabla'], j) + '\n' + str(error))

# COMMAND ----------

# DBTITLE 1,Init
get_uses_cases_by_app(NombreAplicacion, NombreSubAplicacion)
