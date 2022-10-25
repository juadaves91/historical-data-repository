# Databricks notebook source
# MAGIC %md
# MAGIC # Pruebas de Integridad

# COMMAND ----------

import hashlib
import os

# COMMAND ----------

dbutils.widgets.text("Aplicacion", "","")
dbutils.widgets.get("Aplicacion")
App = getArgument("Aplicacion")

application, subapplication = App.split('/')

# COMMAND ----------

# DBTITLE 1,Generación archivo propiedades CSV
# Rutas
path_lectura = '/mnt/app-aldon/' + subapplication + '/raw/'
path_escritura = '/mnt/app-aldon-storage/' + subapplication 
# Eliminar archivo de propiedades existente
dbutils.fs.rm('/dbfs' + path_escritura + 'propiedades_aldon_csv_db.txt', True)
# Generacion archivo de propiedades
with open('/dbfs' + path_escritura + '/propiedades_aldon_csv_db.txt', 'w') as f:
  f.write('fuente|md5|registros\r\n')
  print('fuente|md5|registros')
  print('/dbfs' + path_lectura)
  
  for i in os.listdir('/dbfs' + path_lectura):    
    archivo = '/dbfs' + path_lectura + i
    md5 = hashlib.md5(open(archivo, 'rb').read()).hexdigest()
    print('aqui: ', archivo)
    registros = sum(1 for line in open(archivo))
    f.write(i + '|' + md5 + '|' + str(registros) + '\r\n')
    print(i + '|' + md5 + '|' + str(registros))

# COMMAND ----------

# Definición de Rutas
# path_lectura = '/mnt/lotus-pedidos-storage/subpedidos/'
path_escritura = '/mnt/app-aldon-storage/' + subapplication + '/'
# Generacion archivo de propiedades
with open('/dbfs' + path_escritura + '/propiedades_aldon_anexos_db.txt', 'w') as f:
  f.write('carpeta|md5\r\n')
  print('carpeta|md5')
#   for i in os.listdir('/dbfs'+path_lectura):
#     if i.endswith(".zip"):
#       archivo = '/dbfs'+path_lectura+i
#       md5 = hashlib.md5(open(archivo, 'rb').read()).hexdigest()
#       f.write(i+'|'+md5+'\r\n')
#       print(i+'|'+md5)

# COMMAND ----------


