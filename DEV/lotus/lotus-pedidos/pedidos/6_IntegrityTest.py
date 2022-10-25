# Databricks notebook source
import hashlib
import os

# COMMAND ----------

# DBTITLE 1,Generación archivo propiedades JSON
# Rutas
path_lectura = '/mnt/lotus-pedidos/pedidos/raw/'
path_escritura = '/mnt/lotus-pedidos-storage/pedidos/'
# Eliminar archivo de propiedades existente
dbutils.fs.rm('/dbfs'+path_escritura+'propiedades_lotus_json_db.txt', True)
# Generacion archivo de propiedades
with open('/dbfs'+path_escritura+'/propiedades_lotus_json_db.txt', 'w') as f:
  f.write('fuente|md5|registros\r\n')
  print('fuente|md5|registros')
  for i in os.listdir('/dbfs'+path_lectura):
    archivo = '/dbfs'+path_lectura+i
    md5 = hashlib.md5(open(archivo, 'rb').read()).hexdigest()
    registros = sum(1 for line in open(archivo))
    f.write(i+'|'+md5+'|'+str(registros)+'\r\n')
    print(i+'|'+md5+'|'+str(registros))

# COMMAND ----------

# Definición de Rutas
path_lectura = '/mnt/lotus-pedidos-storage/pedidos/'
path_escritura = '/mnt/lotus-pedidos-storage/pedidos/'
# Eliminar archivo de propiedades existente
dbutils.fs.rm('/dbfs'+path_escritura+'propiedades_lotus_anexos_db.txt', True)
# Generacion archivo de propiedades
with open('/dbfs'+path_escritura+'/propiedades_lotus_anexos_db.txt', 'w') as f:
  f.write('carpeta|md5\r\n')
  print('carpeta|md5')
  for i in os.listdir('/dbfs'+path_lectura):
    if i.endswith(".zip"):
      archivo = '/dbfs'+path_lectura+i
      md5 = hashlib.md5(open(archivo, 'rb').read()).hexdigest()
      f.write(i+'|'+md5+'\r\n')
      print(i+'|'+md5)