# Databricks notebook source
import hashlib
import os

# COMMAND ----------

dbutils.widgets.text("Aplicacion", "","")
dbutils.widgets.get("Aplicacion")
application = getArgument("Aplicacion")

# COMMAND ----------

# Rutas
path_lectura = '/mnt/app-' + application  + '/raw/'
path_escritura = '/mnt/app-' + application + '-storage/' 
# Generacion archivo de propiedades
with open('/dbfs' + path_escritura + '/propiedades_' + application + '_csv_db.txt', 'w') as f:
  f.write('fuente|md5|registros\r\n')
  print('fuente|md5|registros')
  for i in os.listdir('/dbfs' + path_lectura):
    archivo = '/dbfs' + path_lectura + i
    md5 = hashlib.md5(open(archivo, 'rb').read()).hexdigest()
    print('ruta: ', archivo)
    registros = sum(1 for line in open(archivo))
    f.write(i + '|' + md5 + '|' + str(registros) + '\r\n')
    print(i + '|' + md5 + '|' + str(registros))

# COMMAND ----------

# Definición de Rutas
path_escritura = '/mnt/app-' + application + '-storage/'
# Generacion archivo de propiedades
with open('/dbfs' + path_escritura + 'propiedades_' + application + '_anexos_db.txt', 'w') as f:
  f.write('carpeta|md5\r\n')
  print('carpeta|md5')
#   for i in os.listdir('/dbfs'+path_lectura):
#     if i.endswith(".zip"):
#       archivo = '/dbfs'+path_lectura+i
#       md5 = hashlib.md5(open(archivo, 'rb').read()).hexdigest()
#       f.write(i+'|'+md5+'\r\n')
#       print(i+'|'+md5)
