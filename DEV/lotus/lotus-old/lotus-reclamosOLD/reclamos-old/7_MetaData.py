# Databricks notebook source
import pandas as pd
import numpy as np
path_escritura = '/mnt/lotus-reclamos-storage/reclamos-old'
# Eliminar archivo de propiedades existente
dbutils.fs.rm("/dbfs"+path_escritura+"/metadata_lotus_db.txt", True)
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
# Describe de la tabla
meta = spark.sql("""DESCRIBE PROCESS.Zdp_Lotus_Reclamos_Reclamos_TablaIntermedia_Old""")
# Conversion a Pandas Dataframe
pmeta = meta.select("*").toPandas()
pmeta.to_csv("/dbfs"+path_escritura+"/metadata_lotus_db.txt", index=False, columns=['col_name', 'data_type'], sep='|')