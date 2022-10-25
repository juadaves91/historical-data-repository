# Databricks notebook source
import requests
import ast

# COMMAND ----------

name_table = "LotusJson"
sasToken = dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "SasStorageAccount")
path_rdh_auditoria = "/mnt/rdh-auditoria/results/zdr_lotus_table_storage_json"
headers = {'Accept': 'application/json'}
domain = "https://historicosdesasac01.table.core.windows.net/"

# COMMAND ----------

request = requests.get((domain + name_table + sasToken), headers=headers)

# COMMAND ----------

def ft_get_df_table_storage():
  try:
      request = requests.get((domain + name_table + sasToken), headers=headers)
      if request.status_code == 200:
        content =  request.content
        data = ast.literal_eval(content.decode('utf-8'))
        sc = spark.sparkContext
        schema_rdd = sc.parallelize(data["value"])
        return sqlContext.createDataFrame(schema_rdd)
      else:
        raise Exception("Falla la consulta en la table storage " + name_table)
        
  except Exception as error:
    print(error, "Error en el get table storage")

# COMMAND ----------

ft_get_df_table_storage().select("PartitionKey","RowKey", "LoadDate", "Registros").write.mode("overwrite").option("path" , path_rdh_auditoria).saveAsTable("auditoria.zdr_lotus_table_storage_json")

# COMMAND ----------

df = sqlContext.sql("select PartitionKey, MAX(LoadDate) as LoadDate, TRIM(lower(RowKey)) as RowKey,   \
                     Registros from auditoria.zdr_lotus_table_storage_json group by PartitionKey, RowKey, Registros order by RowKey")
sqlContext.sql("drop table if exists auditoria.zdr_lotus_table_storage_json")
df.write.mode("overwrite").option("path" , path_rdh_auditoria).saveAsTable("auditoria.zdr_lotus_table_storage_json")

# COMMAND ----------

def ft_get_records_table_storage(ar_rowkey):
  try:
    registros = sqlContext.sql("select Registros from auditoria.zdr_lotus_table_storage_json where RowKey = " + "'" + ar_rowkey.strip().lower() + "'")
    df_f = registros.toPandas()
    return df_f["Registros"][0]
  except Exception as error:
    print(error, "Error al devolver la cantidad de registros del JSON")
