# Databricks notebook source
from pyspark.sql.functions import col, substring, substring_index,array_join

# COMMAND ----------

# MAGIC %run /utilidades/SchemaApp

# COMMAND ----------

def ft_auditoria():
  lectura = open("/dbfs/mnt/utilidades/execute_json_auditoria.txt","r")
  return lectura.readline()

# COMMAND ----------

ValorX = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_URL_BLOBSTORAGE' LIMIT 1").first()["Valor1"]

# COMMAND ----------

def ft_cayman(ar_path):
  try:
    cayman_df = spark.read.json(ar_path)
    sqlContext.registerDataFrameAsTable(cayman_df, "Zdc_Lotus_Cayman_TablaIntermedia")
    
    return cayman_df, cayman_df, cayman_df  
  except Exception as Error:
    print(Error, "\nError validando auditoria Cayman")

# COMMAND ----------

def ft_comex(ar_pah):
  try:
    comex_df = spark.read.schema(ft_schema_comex().schema).json(ar_pah)
    sqlContext.registerDataFrameAsTable(comex_df, "Zdc_Lotus_Comex_TablaIntermedia_Aux")

    comex_df = sqlContext.sql("\
    SELECT \
      UniversalIDCompuesto, \
      UniversalID, \
      attachments, \
      Anexos, \
      Autor, \
      Body, \
      CodEvento, \
      CodMoneda, \
      CodProductoServicio, \
      Consecutivo, \
      Consecutivo_1, \
      Estado, \
      EstadoLec, \
      Evento, \
      FechaHora, \
      Moneda, \
      Nit, \
      ProductoServicio, \
      Radicacion, \
      TituloDB, \
      Valor, \
      Valor_1 \
    FROM \
      Zdc_Lotus_Comex_TablaIntermedia_Aux")

    sqlContext.registerDataFrameAsTable(comex_df, "Zdc_Lotus_Comex_TablaIntermedia")

    """CodResponsable como String"""
    comex_CodResponsable_string_payload =  ["{'UniversalID':'string','UniversalIDCompuesto':'string','CodResponsable':{'label':'string','value':'string'}}"]

    sc = spark.sparkContext
    comex_CodResponsable_string_rdd = sc.parallelize(comex_CodResponsable_string_payload)
    comex_CodResponsable_string_json = spark.read.json(comex_CodResponsable_string_rdd)
    comex_CodResponsable_string_df = spark.read.schema(comex_CodResponsable_string_json.schema).json(ar_pah)
    comex_CodResponsable_string_df = comex_CodResponsable_string_df.selectExpr("UniversalID","UniversalIDCompuesto","CodResponsable.value as CodResponsable")
    comex_CodResponsable_string_df = comex_CodResponsable_string_df.filter(comex_CodResponsable_string_df.CodResponsable.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(comex_CodResponsable_string_df, "Zdc_Lotus_Comex_Tablaintermedia_CodResponsable_string")

    """CodResponsable como Array"""
    comex_CodResponsable_array_payload = ["{'UniversalID':'string','UniversalIDCompuesto':'string','CodResponsable':{'label':'string', 'value':['array']}}"]
    sc = spark.sparkContext
    comex_CodResponsable_array_rdd = sc.parallelize(comex_CodResponsable_array_payload)
    comex_CodResponsable_array_json = spark.read.json(comex_CodResponsable_array_rdd)
    comex_CodResponsable_array_df = spark.read.schema(comex_CodResponsable_array_json.schema).json(ar_pah)
    comex_CodResponsable_array_df = comex_CodResponsable_array_df.selectExpr("UniversalID","UniversalIDCompuesto","CodResponsable.value as CodResponsable")
    comex_CodResponsable_array_df = comex_CodResponsable_array_df.filter(col("UniversalID").isNotNull())
    comex_CodResponsable_array_df = comex_CodResponsable_array_df.select("UniversalID","UniversalIDCompuesto", array_join("CodResponsable", '\r\n ').alias("CodResponsable"))
    sqlContext.registerDataFrameAsTable(comex_CodResponsable_array_df, "Zdc_Lotus_Comex_Tablaintermedia_CodResponsable_array")

    comex_CodResponsable_df = sqlContext.sql("SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, CodResponsable FROM Zdc_Lotus_Comex_Tablaintermedia_CodResponsable_string UNION SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto)As UniversalID, CodResponsable FROM Zdc_Lotus_Comex_Tablaintermedia_CodResponsable_array")
    sqlContext.registerDataFrameAsTable(comex_CodResponsable_df, "Zdc_Lotus_Comex_Tablaintermedia_CodResponsable")

    """Responsable como String"""
    comex_Responsable_string_payload =  ["{'UniversalID':'string','UniversalIDCompuesto':'string','Responsable':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    comex_Responsable_string_rdd = sc.parallelize(comex_Responsable_string_payload)
    comex_Responsable_string_json = spark.read.json(comex_Responsable_string_rdd)
    comex_Responsable_string_df = spark.read.schema(comex_Responsable_string_json.schema).json(ar_pah)
    comex_Responsable_string_df = comex_Responsable_string_df.selectExpr("UniversalID","UniversalIDCompuesto","Responsable.value as Responsable")
    comex_Responsable_string_df = comex_Responsable_string_df.filter(comex_Responsable_string_df.Responsable.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(comex_Responsable_string_df, "Zdc_Lotus_Comex_Tablaintermedia_Responsable_string")

    """Responsable como Array"""
    comex_Responsable_array_payload = ["{'UniversalID':'string','UniversalIDCompuesto':'string','Responsable':{'label':'string', 'value':['array']}}"]

    sc = spark.sparkContext
    comex_Responsable_array_rdd = sc.parallelize(comex_Responsable_array_payload)
    comex_Responsable_array_json = spark.read.json(comex_Responsable_array_rdd)
    comex_Responsable_array_df = spark.read.schema(comex_Responsable_array_json.schema).json(ar_pah)
    comex_Responsable_array_df = comex_Responsable_array_df.selectExpr("UniversalID","UniversalIDCompuesto","Responsable.value as Responsable")
    comex_Responsable_array_df = comex_Responsable_array_df.filter(col("UniversalID").isNotNull())
    comex_Responsable_array_df = comex_Responsable_array_df.select("UniversalID","UniversalIDCompuesto", array_join("Responsable", '\r\n ').alias("Responsable"))
    sqlContext.registerDataFrameAsTable(comex_Responsable_array_df, "Zdc_Lotus_Comex_Tablaintermedia_Responsable_array")

    comex_Responsable_df = sqlContext.sql("SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, Responsable FROM Zdc_Lotus_Comex_Tablaintermedia_Responsable_string UNION SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID,Responsable FROM Zdc_Lotus_Comex_Tablaintermedia_Responsable_array")
    sqlContext.registerDataFrameAsTable(comex_Responsable_df, "Zdc_Lotus_Comex_Tablaintermedia_Responsable")

    """Historia como String"""
    comex_Historia_string_payload =  ["{'UniversalID':'string','UniversalIDCompuesto':'string','Historia':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    comex_Historia_string_rdd = sc.parallelize(comex_Historia_string_payload)
    comex_Historia_string_json = spark.read.json(comex_Historia_string_rdd)
    comex_Historia_string_df = spark.read.schema(comex_Historia_string_json.schema).json(ar_pah)
    comex_Historia_string_df = comex_Historia_string_df.selectExpr("UniversalID","UniversalIDCompuesto","Historia.value as Historia")
    comex_Historia_string_df = comex_Historia_string_df.filter(comex_Historia_string_df.Historia.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(comex_Historia_string_df, "Zdc_Lotus_Comex_Tablaintermedia_Historia_string")

    """Historia como Array"""
    comex_Historia_array_payload = ["{'UniversalID':'string','UniversalIDCompuesto':'string','Historia':{'label':'string', 'value':['array']}}"]
    sc = spark.sparkContext
    comex_Historia_array_rdd = sc.parallelize(comex_Historia_array_payload)
    comex_Historia_array_json = spark.read.json(comex_Historia_array_rdd)
    comex_Historia_array_df = spark.read.schema(comex_Historia_array_json.schema).json(ar_pah)
    comex_Historia_array_df = comex_Historia_array_df.selectExpr("UniversalID","UniversalIDCompuesto","Historia.value as Historia")
    comex_Historia_array_df = comex_Historia_array_df.filter(col("UniversalID").isNotNull())
    comex_Historia_array_df = comex_Historia_array_df.select("UniversalID","UniversalIDCompuesto", array_join("Historia", '\r\n ').alias("Historia"))
    sqlContext.registerDataFrameAsTable(comex_Historia_array_df, "Zdc_Lotus_Comex_Tablaintermedia_Historia_array")

    comex_Historia_df = sqlContext.sql("SELECT if(UniversalIDCompuesto is Null,UniversalID , UniversalIDCompuesto) As UniversalID, Historia FROM Zdc_Lotus_Comex_Tablaintermedia_Historia_string UNION SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, Historia FROM Zdc_Lotus_Comex_Tablaintermedia_Historia_array")
    sqlContext.registerDataFrameAsTable(comex_Historia_df, "Zdc_Lotus_Comex_Tablaintermedia_Historia")
    
    ValorY = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_LOTUS_COMEX_APP' LIMIT 1").first()["Valor1"]

    sql_query = "SELECT \
       UniversalID, \
       UniversalIDCompuesto, \
       TituloDB, \
       Nit.value Nit, \
       CodProductoServicio.value CodProductoServicio, \
       ProductoServicio.value ProductoServicio, \
       CodEvento.value CodEvento, \
       Evento.value Evento, \
       Valor.value Valor, \
       Valor_1.value Valor1, \
       Consecutivo.value Consecutivo, \
       FechaHora.value FechaHora, \
       IF (length(concat_ws(',', attachments)) > 0, CONCAT('" + ValorX + ValorY + "/', UniversalID,'.zip'" + "), '') link, \
       translate(concat(concat(Nit.value, ' - ', CodProductoServicio.value, ' - ', ProductoServicio.value, ' - ', Evento.value, ' - ', Valor.value,' - ', Valor_1.value,' - ',Consecutivo.value), ' - ', upper(concat(ProductoServicio.value, ' - ', 'Evento.value')), ' - ', lower(concat(ProductoServicio.value, ' - ', 'Evento.value'))),'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN') busqueda, \
       cast(substr(substring_index(FechaHora.value, ' ', 1),-4,4) as int) AS anio, \
       cast(substr(substring_index(FechaHora.value, ' ', 1),-7,2) as int) AS mes \
    FROM \
      Zdc_Lotus_Comex_TablaIntermedia"

    panel_principal_comex = sqlContext.sql(sql_query)

    panel_detalle_comex = sqlContext.sql("\
    SELECT UniversalID, 'Encabezado' AS vista, 1 as posicion, 'Creado Por' AS key, Autor.value AS value, cast(substr(substring_index(FechaHora.value, ' ', 1),-4,4)as int) AS Anio, cast(substr(substring_index(FechaHora.value, ' ', 1),-7,2)as int) AS Mes FROM Zdc_Lotus_Comex_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Encabezado' AS vista, 2 as posicion, 'Referencia' AS key, Radicacion.value AS value, cast(substr(substring_index(FechaHora.value, ' ', 1),-4,4)as int) AS Anio, cast(substr(substring_index(FechaHora.value, ' ', 1),-7,2)as int) AS Mes FROM Zdc_Lotus_Comex_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Información General' AS vista, 3 as posicion, 'Consecutivo' AS key, consecutivo.value AS value, cast(substr(substring_index(FechaHora.value, ' ', 1),-4,4)as int) AS Anio, cast(substr(substring_index(FechaHora.value, ' ', 1),-7,2)as int) AS Mes FROM Zdc_Lotus_Comex_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Información General' AS vista, 4 as posicion, 'Consecutivo' AS key, consecutivo_1.value AS value, cast(substr(substring_index(FechaHora.value, ' ', 1),-4,4)as int) AS Anio, cast(substr(substring_index(FechaHora.value, ' ', 1),-7,2)as int) AS Mes FROM Zdc_Lotus_Comex_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Información General' AS vista, 5 as posicion, 'Moneda' AS key, CodMoneda.value AS value, cast(substr(substring_index(FechaHora.value, ' ', 1),-4,4)as int) AS Anio, cast(substr(substring_index(FechaHora.value, ' ', 1),-7,2)as int) AS Mes FROM Zdc_Lotus_Comex_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Información General' AS vista, 6 as posicion, 'Moneda' AS key, Moneda.value AS value, cast(substr(substring_index(FechaHora.value, ' ', 1),-4,4)as int) AS Anio, cast(substr(substring_index(FechaHora.value, ' ', 1),-7,2)as int) AS Mes FROM Zdc_Lotus_Comex_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Contenido' AS vista, 7 as posicion, '' AS key, Body.value AS value, cast(substr(substring_index(FechaHora.value, ' ', 1),-4,4)as int) AS Anio, cast(substr(substring_index(FechaHora.value, ' ', 1),-7,2)as int) AS Mes FROM Zdc_Lotus_Comex_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Estado' AS vista, 8 as posicion, '' AS key, Estado.value AS value, cast(substr(substring_index(FechaHora.value, ' ', 1),-4,4)as int) AS Anio, cast(substr(substring_index(FechaHora.value, ' ', 1),-7,2)as int) AS Mes FROM Zdc_Lotus_Comex_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Estado' AS vista, 9 as posicion, '' AS key, EstadoLec.value AS value, cast(substr(substring_index(FechaHora.value, ' ', 1),-4,4)as int) AS Anio, cast(substr(substring_index(FechaHora.value, ' ', 1),-7,2)as int) AS Mes FROM Zdc_Lotus_Comex_TablaIntermedia \
    UNION ALL \
    SELECT ZC.UniversalId, 'Estado' AS vista, 10 as posicion, 'Responsable' AS key, ZC.CodResponsable AS value, cast(substr(substring_index(ZT.FechaHora.value, ' ', 1),-4,4)as int) AS Anio, cast(substr(substring_index(ZT.FechaHora.value, ' ', 1),-7,2)as int) AS Mes FROM Zdc_Lotus_Comex_Tablaintermedia_CodResponsable ZC \
    INNER JOIN Zdc_Lotus_Comex_TablaIntermedia ZT ON ZC.UniversalID = ZT.UniversalID \
    UNION ALL \
    SELECT ZC.UniversalId, 'Estado' AS vista, 11 as posicion, 'Responsable' AS key, ZC.Responsable AS value, cast(substr(substring_index(ZT.FechaHora.value, ' ', 1),-4,4)as int) AS Anio, cast(substr(substring_index(ZT.FechaHora.value, ' ', 1),-7,2)as int) AS Mes FROM Zdc_Lotus_Comex_Tablaintermedia_Responsable ZC INNER JOIN Zdc_Lotus_Comex_TablaIntermedia ZT ON ZC.UniversalID = ZT.UniversalID \
    UNION ALL \
    SELECT UniversalID, 'Anexos' AS vista, 12 as posicion, '' AS key, Anexos.value AS value, cast(substr(substring_index(FechaHora.value, ' ', 1),-4,4)as int) AS Anio, cast(substr(substring_index(FechaHora.value, ' ', 1),-7,2)as int) AS Mes FROM Zdc_Lotus_Comex_TablaIntermedia \
    UNION ALL \
    SELECT ZC.UniversalId, 'Registro Histórico' AS vista, 13 as posicion, '' AS key, ZC.Historia AS value, cast(substr(substring_index(ZT.FechaHora.value, ' ', 1),-4,4)as int) AS Anio, cast(substr(substring_index(ZT.FechaHora.value, ' ', 1),-7,2)as int) AS Mes FROM Zdc_Lotus_Comex_Tablaintermedia_Historia ZC \
    INNER JOIN Zdc_Lotus_Comex_TablaIntermedia ZT ON ZC.UniversalID = ZT.UniversalID")

    return comex_df, panel_principal_comex, panel_detalle_comex
  except Exception as Error:
    print(Error, "\nError validando auditoria Comex")

# COMMAND ----------

def ft_pedidos(ar_path):
  try:
    pedidos_df = spark.read.schema(ft_schema_pedidos().schema).json(ar_path)
    sqlContext.registerDataFrameAsTable(pedidos_df, "Zdc_Lotus_Pedidos_TablaIntermedia")

    pedidos_df = pedidos_df.select(col("UniversalID").alias("UniversalIDPadre"),
                                   col("UniversalIDCompuesto").alias("UniversalID"),
                                   col("attachments").alias("attachments"),
                                   col("rch_DetallePed.value").alias("rch_DetallePed"),
                                   col("txt_Autor.value").alias("txt_Autor"),
                                   col("txt_Creador.value").alias("txt_Creador"),
                                   col("dat_FchCreac.value").alias("dat_FchCreac"),
                                   col("key_TipoEnteExt.value").alias("key_TipoEnteExt"),
                                   col("key_NomPedido.value").alias("key_NomPedido"),
                                   col("key_Prioridad.value").alias("key_Prioridad"),
                                   col("txt_AsuntoPed.value").alias("txt_AsuntoPed"),
                                   col("dat_FchMaxEstado.value").alias("dat_FchMaxEstado"),
                                   col("txt_ValidarResp.value").alias("txt_ValidarResp"),
                                   col("txt_Comentarios.value").alias("txt_Comentarios"),
                                   col("rch_AnexosPed.value").alias("rch_AnexosPed"),
                                   col("rch_AnexosAdic.value").alias("rch_AnexosAdic"),
                                   col("txt_Titulo1.value").alias("txt_Titulo1"),
                                   col("txt_Titulo2.value").alias("txt_Titulo2"),
                                   col("txt_NomCompEmp_A.value").alias("txt_NomCompEmp_A"),
                                   col("txt_DepEmp_A.value").alias("txt_DepEmp_A"),
                                   col("txt_RgnEmp_A.value").alias("txt_RgnEmp_A"),
                                   col("txt_UbicEmp_A.value").alias("txt_UbicEmp_A"),
                                   col("txt_ExtEmp_A.value").alias("txt_ExtEmp_A"),
                                   col("txt_EmpEnteExt.value").alias("txt_EmpEnteExt"),
                                   col("txt_TelEnteExt.value").alias("txt_TelEnteExt"),
                                   col("dat_FchIniTram.value").alias("dat_FchIniTram"),
                                   col("num_DVTotal.value").alias("num_DVTotal"),
                                   col("num_DVParcial.value").alias("num_DVParcial"),
                                   col("key_Prioridad_1.value").alias("key_Prioridad_1"),
                                   col("txt_Estado.value").alias("txt_Estado"),
                                   col("dat_FchEstadoAct.value").alias("dat_FchEstadoAct"),
                                   col("num_TMaxEstado.value").alias("num_TMaxEstado"),
                                   col("dat_FchMaxEstado_1.value").alias("dat_FchMaxEstado_1"),
                                   col("num_TCotizado.value").alias("num_TCotizado"),
                                   col("num_TCotizado_1.value").alias("num_TCotizado_1"),
                                   col("num_PesosCotiz.value").alias("num_PesosCotiz"),
                                   col("num_PesosCotiz_1.value").alias("num_PesosCotiz_1"),
                                   col("dat_FchVencEstado.value").alias("dat_FchVencEstado"),
                                   col("txt_Responsable.value").alias("txt_Responsable"),
                                   col("txt_Responsable_1.value").alias("txt_Responsable_1"),
                                   col("txt_NomCompEmp_R.value").alias("txt_NomCompEmp_R"),
                                   col("txt_EmpresaEmp_R.value").alias("txt_EmpresaEmp_R"),
                                   col("txt_DepEmp_R.value").alias("txt_DepEmp_R"),
                                   col("txt_RgnEmp_R.value").alias("txt_RgnEmp_R"),
                                   col("txt_UbicEmp_R.value").alias("txt_UbicEmp_R"),
                                   col("txt_ExtEmp_R.value").alias("txt_ExtEmp_R"),
                                   substring(substring_index('dat_FchCreac.value', ' ', 1),-4,4).cast('int').alias('anio'),
                                   substring(substring_index('dat_FchCreac.value', ' ', 1),-7,2).cast('int').alias('mes'))

    sqlContext.registerDataFrameAsTable(pedidos_df, "Zdc_Lotus_Pedidos_TablaIntermedia")

    """txt_Historia como String"""
    pedidos_txt_Historia_string_payload =  ["{'UniversalIDCompuesto':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_Historia':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    pedidos_txt_Historia_string_rdd = sc.parallelize(pedidos_txt_Historia_string_payload)
    pedidos_txt_Historia_string_json = spark.read.json(pedidos_txt_Historia_string_rdd)
    pedidos_txt_Historia_string_df = spark.read.schema(pedidos_txt_Historia_string_json.schema).json(ar_path)
    pedidos_txt_Historia_string_df = pedidos_txt_Historia_string_df.selectExpr("UniversalIDCompuesto as UniversalID" ,"dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
    pedidos_txt_Historia_string_df = pedidos_txt_Historia_string_df.na.fill('')
    pedidos_txt_Historia_string_df = pedidos_txt_Historia_string_df.filter(pedidos_txt_Historia_string_df.txt_Historia.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(pedidos_txt_Historia_string_df, "Zdc_Lotus_Pedidos_TablaIntermedia_txt_Historia_string")

    """txt_Historia como Array"""
    pedidos_txt_Historia_array_payload = ["{'UniversalIDCompuesto':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_Historia':{'Label':'string','value':['array']}}"]
    sc = spark.sparkContext
    pedidos_txt_Historia_array_rdd = sc.parallelize(pedidos_txt_Historia_array_payload)
    pedidos_txt_Historia_array_json = spark.read.json(pedidos_txt_Historia_array_rdd)
    pedidos_txt_Historia_array_df = spark.read.schema(pedidos_txt_Historia_array_json.schema).json(ar_path)
    pedidos_txt_Historia_array_df = pedidos_txt_Historia_array_df.selectExpr("UniversalIDCompuesto as UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
    pedidos_txt_Historia_array_df = pedidos_txt_Historia_array_df.filter(col("UniversalID").isNotNull())
    pedidos_txt_Historia_array_df = pedidos_txt_Historia_array_df.select("UniversalID","dat_FchCreac", array_join("txt_Historia", '\r\n ').alias("txt_Historia"))
    pedidos_txt_Historia_array_df = pedidos_txt_Historia_array_df.na.fill('')
    sqlContext.registerDataFrameAsTable(pedidos_txt_Historia_array_df, "Zdc_Lotus_pedidos_Tablaintermedia_txt_Historia_array")

    pedidos_txt_Historia_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Pedidos_TablaIntermedia_txt_Historia_string UNION SELECT * FROM Zdc_Lotus_pedidos_Tablaintermedia_txt_Historia_array")
    sqlContext.registerDataFrameAsTable(pedidos_txt_Historia_df, "Zdc_Lotus_Pedidos_Tablaintermedia_txt_Historia")

    ValorY = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_LOTUS_PEDIDOS_APP' LIMIT 1").first()["Valor1"]

    panel_principal_pedidos = sqlContext.sql ("\
    SELECT \
      UniversalID, \
      UniversalIDPadre, \
      key_NomPedido, \
      txt_Autor Autor, \
      txt_AsuntoPed, \
      dat_FchCreac, \
      IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/pedidos/', UniversalID,'.zip'" + "), '') link, \
      translate(concat(concat(if(key_NomPedido is null, '',key_NomPedido), ' - ', \
                              if(txt_Autor is null, '',txt_Autor), ' - ', \
                              if(dat_FchCreac is null, '',dat_FchCreac), ' - ', \
                              if(txt_AsuntoPed is null, '',txt_AsuntoPed)), ' - ',  \
      upper(concat(if(key_NomPedido is null, '',key_NomPedido), ' - ', \
                   if(txt_Autor is null, '',txt_Autor), ' - ', \
                   if(txt_AsuntoPed is null, '',txt_AsuntoPed))), ' - ', \
      lower(concat(if(key_NomPedido is null, '',key_NomPedido), ' - ', \
                   if(txt_Autor is null, '',txt_Autor), ' - ', \
                   if(txt_AsuntoPed is null, '',txt_AsuntoPed)))),'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN')  AS busqueda, \
      anio, \
      mes \
    FROM \
      Zdc_Lotus_Pedidos_TablaIntermedia")

    panel_detalle_pedidos = sqlContext.sql("\
    SELECT UniversalID, 'Encabezado' AS vista, 1 as posicion, 'Autor' AS key, txt_Autor AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Encabezado' AS vista, 2 as posicion, 'Fecha Creación' AS key, dat_FchCreac AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Información del Empleado Solicitante' AS vista, 3 as posicion, 'Nombres' AS key, txt_NomCompEmp_A AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Información del Empleado Solicitante' AS vista, 4 as posicion, 'Dependencia' AS key, txt_DepEmp_A AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Información del Empleado Solicitante' AS vista, 5 as posicion, 'Región' AS key, txt_RgnEmp_A AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Información del Empleado Solicitante' AS vista, 6 as posicion, 'Ubicación' AS key, txt_UbicEmp_A AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Información del Empleado Solicitante' AS vista, 7 as posicion, 'Teléfono' AS key, txt_TelEnteExt AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Información del Empleado Solicitante' AS vista, 8 as posicion, 'Fecha de Entrega de Definición' AS key, dat_FchIniTram AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Datos Generales' AS vista, 9 as posicion, 'Tipo Pedido' AS key, key_NomPedido AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Datos Generales' AS vista, 10 as posicion, 'Prioridad' AS key, key_Prioridad AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Datos Generales' AS vista, 11 as posicion, 'Asunto' AS key, txt_AsuntoPed AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Estado y Acuerdos de Servicio' AS vista, 12 as posicion, 'Estado' AS key, txt_Estado AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Estado y Acuerdos de Servicio' AS vista, 13 as posicion, 'Fecha del Estado' AS key, dat_FchEstadoAct AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Asignación de Responsable' AS vista, 14 as posicion, 'Responsable' AS key, txt_Responsable AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Información del Empleado Responsable' AS vista, 15 as posicion, 'Nombres' AS key, txt_NomCompEmp_R AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Información del Empleado Responsable' AS vista, 16 as posicion, 'Empresa' AS key, txt_EmpresaEmp_R AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Información del Empleado Responsable' AS vista, 17 as posicion, 'Dependencia' AS key, txt_DepEmp_R AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Información del Empleado Responsable' AS vista, 18 as posicion, 'Región' AS key, txt_RgnEmp_R AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Información del Empleado Responsable' AS vista, 19 as posicion, 'Ubicación' AS key, txt_UbicEmp_R AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Información del Empleado Responsable' AS vista, 20 as posicion, 'Teléfono' AS key, txt_ExtEmp_R AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Comentarios' AS vista, 21 as posicion, '' AS key, txt_Comentarios AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Historia' AS vista, 22 as posicion, '' AS key, txt_Historia AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4)as int) AS Anio,  cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2)as int) AS Mes FROM  Zdc_Lotus_Pedidos_Tablaintermedia_txt_Historia")
    
    return pedidos_df, panel_principal_pedidos, panel_detalle_pedidos
  except Exception as Error:
    print(Error, "\nError validando auditoria Pedidos")

# COMMAND ----------

def ft_subpedidos(ar_path):
  try:
    subpedidos_df = spark.read.schema(ft_schema_subpedidos().schema).json(ar_path)
    subpedidos_df = subpedidos_df.select(col("UniversalID").alias("UniversalIDRespaldo"),
                                         col("attachments").alias("attachments"),
                                         col("UniversalIDCompuesto").alias("UniversalID"),
                                         col("SSREF.value").alias("UniversalIDPadre"),
                                         col("key_ViceAtdPed.value").alias("key_ViceAtdPed"),
                                         col("key_NomPedido.value").alias("key_NomPedido"),
                                         col("txt_Autor.value").alias("txt_Autor"),
                                         col("txt_Creador.value").alias("txt_Creador"),
                                         col("dat_FchCreac.value").alias("dat_FchCreac"),
                                         col("key_TipoEnteExt.value").alias("key_TipoEnteExt"),
                                         col("key_Prioridad.value").alias("key_Prioridad"),
                                         col("txt_AsuntoPed.value").alias("txt_AsuntoPed"),
                                         col("dat_FchMaxEstado.value").alias("dat_FchMaxEstado"),
                                         col("txt_ValidarResp.value").alias("txt_ValidarResp"),
                                         col("rch_DetallePed.value").alias("rch_DetallePed"),
                                         col("txt_Comentarios.value").alias("txt_Comentarios"),
                                         col("rch_AnexosPed.value").alias("rch_AnexosPed"),
                                         col("txt_Titulo1.value").alias("txt_Titulo1"),
                                         col("txt_Titulo2.value").alias("txt_Titulo2"),
                                         col("txt_NomCompEmp_A.value").alias("txt_NomCompEmp_A"),
                                         col("txt_DepEmp_A.value").alias("txt_DepEmp_A"),
                                         col("txt_TelEnteExt.value").alias("txt_TelEnteExt"),
                                         col("dat_FchIniTram.value").alias("dat_FchIniTram"),
                                         col("num_DVTotal.value").alias("num_DVTotal"),
                                         col("num_DVParcial.value").alias("num_DVParcial"),
                                         col("key_Prioridad_1.value").alias("key_Prioridad_1"),
                                         col("txt_Estado.value").alias("txt_Estado"),
                                         col("dat_FchEstadoAct.value").alias("dat_FchEstadoAct"),
                                         col("num_TMaxEstado.value").alias("num_TMaxEstado"),
                                         col("dat_FchMaxEstado_1.value").alias("dat_FchMaxEstado_1"),
                                         col("num_TCotizado.value").alias("num_TCotizado"),
                                         col("num_TCotizado_1.value").alias("num_TCotizado_1"),
                                         col("num_PesosCotiz.value").alias("num_PesosCotiz"),
                                         col("num_PesosCotiz_1.value").alias("num_PesosCotiz_1"),
                                         col("dat_FchVencEstado.value").alias("dat_FchVencEstado"),
                                         col("txt_Responsable.value").alias("txt_Responsable"),
                                         col("txt_Responsable_1.value").alias("txt_Responsable_1"),
                                         col("txt_NomCompEmp_R.value").alias("txt_NomCompEmp_R"),
                                         col("txt_EmpresaEmp_R.value").alias("txt_EmpresaEmp_R"),
                                         col("txt_DepEmp_R.value").alias("txt_DepEmp_R"),
                                         col("txt_RgnEmp_R.value").alias("txt_RgnEmp_R"),
                                         col("txt_UbicEmp_R.value").alias("txt_UbicEmp_R"),
                                         col("txt_ExtEmp_R.value").alias("txt_ExtEmp_R"),
                                         substring(substring_index('dat_FchCreac.value', ' ', 1),-4,4).cast('int').alias('anio'),
                                         substring(substring_index('dat_FchCreac.value', ' ', 1),-7,2).cast('int').alias('mes'))

    sqlContext.registerDataFrameAsTable(subpedidos_df, "Zdc_Lotus_Subpedidos_TablaIntermedia")
    
    """txt_RgnEmp_A como String"""
    subpedidos_txt_RgnEmp_A_string_payload =  ["{'UniversalIDCompuesto':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_RgnEmp_A':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    subpedidos_txt_RgnEmp_A_string_rdd = sc.parallelize(subpedidos_txt_RgnEmp_A_string_payload)
    subpedidos_txt_RgnEmp_A_string_json = spark.read.json(subpedidos_txt_RgnEmp_A_string_rdd)
    subpedidos_txt_RgnEmp_A_string_df = spark.read.schema(subpedidos_txt_RgnEmp_A_string_json.schema).json(ar_path)
    subpedidos_txt_RgnEmp_A_string_df = subpedidos_txt_RgnEmp_A_string_df.selectExpr("UniversalIDCompuesto as UniversalID","dat_FchCreac.value as dat_FchCreac","txt_RgnEmp_A.value as txt_RgnEmp_A")
    subpedidos_txt_RgnEmp_A_string_df = subpedidos_txt_RgnEmp_A_string_df.na.fill('')
    subpedidos_txt_RgnEmp_A_string_df = subpedidos_txt_RgnEmp_A_string_df.filter(subpedidos_txt_RgnEmp_A_string_df.txt_RgnEmp_A.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(subpedidos_txt_RgnEmp_A_string_df, "Zdc_Lotus_Subpedidos_TablaIntermedia_txt_RgnEmp_A_string")
    
    """txt_RgnEmp_A como Array"""
    subpedidos_txt_RgnEmp_A_array_payload = ["{'UniversalIDCompuesto':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_RgnEmp_A':{'Label':'string','value':['array']}}"]
    sc = spark.sparkContext
    subpedidos_txt_RgnEmp_A_array_rdd = sc.parallelize(subpedidos_txt_RgnEmp_A_array_payload)
    subpedidos_txt_RgnEmp_A_array_json = spark.read.json(subpedidos_txt_RgnEmp_A_array_rdd)
    subpedidos_txt_RgnEmp_A_array_df = spark.read.schema(subpedidos_txt_RgnEmp_A_array_json.schema).json(ar_path)
    subpedidos_txt_RgnEmp_A_array_df = subpedidos_txt_RgnEmp_A_array_df.selectExpr("UniversalIDCompuesto as UniversalID","dat_FchCreac.value as dat_FchCreac","txt_RgnEmp_A.value as txt_RgnEmp_A")
    subpedidos_txt_RgnEmp_A_array_df = subpedidos_txt_RgnEmp_A_array_df.filter(col("UniversalID").isNotNull())
    subpedidos_txt_RgnEmp_A_array_df = subpedidos_txt_RgnEmp_A_array_df.select("UniversalID","dat_FchCreac", array_join("txt_RgnEmp_A", '\r\n ').alias("txt_RgnEmp_A"))
    subpedidos_txt_RgnEmp_A_array_df = subpedidos_txt_RgnEmp_A_array_df.na.fill('')
    sqlContext.registerDataFrameAsTable(subpedidos_txt_RgnEmp_A_array_df, "Zdc_Lotus_Subpedidos_Tablaintermedia_txt_RgnEmp_A_array")

    subpedidos_txt_RgnEmp_A_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Subpedidos_TablaIntermedia_txt_RgnEmp_A_string UNION SELECT * FROM Zdc_Lotus_Subpedidos_Tablaintermedia_txt_RgnEmp_A_array")
    sqlContext.registerDataFrameAsTable(subpedidos_txt_RgnEmp_A_df, "Zdc_Lotus_Subpedidos_Tablaintermedia_txt_RgnEmp_A")

    """txt_UbicEmp_A como String"""
    subpedidos_txt_UbicEmp_A_string_payload =  ["{'UniversalIDCompuesto':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_UbicEmp_A':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    subpedidos_txt_UbicEmp_A_string_rdd = sc.parallelize(subpedidos_txt_UbicEmp_A_string_payload)
    subpedidos_txt_UbicEmp_A_string_json = spark.read.json(subpedidos_txt_UbicEmp_A_string_rdd)
    subpedidos_txt_UbicEmp_A_string_df = spark.read.schema(subpedidos_txt_UbicEmp_A_string_json.schema).json(ar_path)
    subpedidos_txt_UbicEmp_A_string_df = subpedidos_txt_UbicEmp_A_string_df.selectExpr("UniversalIDCompuesto as UniversalID","dat_FchCreac.value as dat_FchCreac","txt_UbicEmp_A.value as txt_UbicEmp_A")
    subpedidos_txt_UbicEmp_A_string_df = subpedidos_txt_UbicEmp_A_string_df.na.fill('')
    subpedidos_txt_UbicEmp_A_string_df = subpedidos_txt_UbicEmp_A_string_df.filter(subpedidos_txt_UbicEmp_A_string_df.txt_UbicEmp_A.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(subpedidos_txt_UbicEmp_A_string_df, "Zdc_Lotus_Subpedidos_TablaIntermedia_txt_UbicEmp_A_string")

    """txt_UbicEmp_A como Array"""
    subpedidos_txt_UbicEmp_A_array_payload = ["{'UniversalIDCompuesto':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_UbicEmp_A':{'Label':'string','value':['array']}}"]
    sc = spark.sparkContext
    subpedidos_txt_UbicEmp_A_array_rdd = sc.parallelize(subpedidos_txt_UbicEmp_A_array_payload)
    subpedidos_txt_UbicEmp_A_array_json = spark.read.json(subpedidos_txt_UbicEmp_A_array_rdd)
    subpedidos_txt_UbicEmp_A_array_df = spark.read.schema(subpedidos_txt_UbicEmp_A_array_json.schema).json(ar_path)
    subpedidos_txt_UbicEmp_A_array_df = subpedidos_txt_UbicEmp_A_array_df.selectExpr("UniversalIDCompuesto as UniversalID","dat_FchCreac.value as dat_FchCreac","txt_UbicEmp_A.value as txt_UbicEmp_A")
    subpedidos_txt_UbicEmp_A_array_df = subpedidos_txt_UbicEmp_A_array_df.filter(col("UniversalID").isNotNull())
    subpedidos_txt_UbicEmp_A_array_df = subpedidos_txt_UbicEmp_A_array_df.select("UniversalID","dat_FchCreac", array_join("txt_UbicEmp_A", '\r\n ').alias("txt_UbicEmp_A"))
    subpedidos_txt_UbicEmp_A_array_df = subpedidos_txt_UbicEmp_A_array_df.na.fill('')
    sqlContext.registerDataFrameAsTable(subpedidos_txt_UbicEmp_A_array_df, "Zdc_Lotus_Subpedidos_Tablaintermedia_txt_UbicEmp_A_array")
    
    subpedidos_txt_UbicEmp_A_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Subpedidos_TablaIntermedia_txt_UbicEmp_A_string UNION SELECT * FROM Zdc_Lotus_Subpedidos_Tablaintermedia_txt_UbicEmp_A_array")
    sqlContext.registerDataFrameAsTable(subpedidos_txt_UbicEmp_A_df, "Zdc_Lotus_Subpedidos_Tablaintermedia_txt_UbicEmp_A")
    
    """txt_Historia como String"""
    subpedidos_txt_Historia_string_payload =  ["{'UniversalIDCompuesto':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_Historia':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    subpedidos_txt_Historia_string_rdd = sc.parallelize(subpedidos_txt_Historia_string_payload)
    subpedidos_txt_Historia_string_json = spark.read.json(subpedidos_txt_Historia_string_rdd)
    subpedidos_txt_Historia_string_df = spark.read.schema(subpedidos_txt_Historia_string_json.schema).json(ar_path)

    subpedidos_txt_Historia_string_df = subpedidos_txt_Historia_string_df.selectExpr("UniversalIDCompuesto as UniversalID" ,"dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
    subpedidos_txt_Historia_string_df = subpedidos_txt_Historia_string_df.na.fill('')
    subpedidos_txt_Historia_string_df = subpedidos_txt_Historia_string_df.filter(subpedidos_txt_Historia_string_df.txt_Historia.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(subpedidos_txt_Historia_string_df, "Zdc_Lotus_Subpedidos_TablaIntermedia_txt_Historia_string")
    
    """txt_Historia como Array"""
    subpedidos_txt_Historia_array_payload = ["{'UniversalIDCompuesto':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_Historia':{'Label':'string','value':['array']}}"]
    sc = spark.sparkContext
    subpedidos_txt_Historia_array_rdd = sc.parallelize(subpedidos_txt_Historia_array_payload)
    subpedidos_txt_Historia_array_json = spark.read.json(subpedidos_txt_Historia_array_rdd)
    subpedidos_txt_Historia_array_df = spark.read.schema(subpedidos_txt_Historia_array_json.schema).json(ar_path)
    subpedidos_txt_Historia_array_df = subpedidos_txt_Historia_array_df.selectExpr("UniversalIDCompuesto as UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
    subpedidos_txt_Historia_array_df = subpedidos_txt_Historia_array_df.filter(col("UniversalID").isNotNull())
    subpedidos_txt_Historia_array_df = subpedidos_txt_Historia_array_df.select("UniversalID","dat_FchCreac", array_join("txt_Historia", '\r\n ').alias("txt_Historia"))
    subpedidos_txt_Historia_array_df = subpedidos_txt_Historia_array_df.na.fill('')
    sqlContext.registerDataFrameAsTable(subpedidos_txt_Historia_array_df, "Zdc_Lotus_subpedidos_Tablaintermedia_txt_Historia_array")
    
    subpedidos_txt_Historia_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Subpedidos_TablaIntermedia_txt_Historia_string UNION SELECT * FROM Zdc_Lotus_subpedidos_Tablaintermedia_txt_Historia_array")
    sqlContext.registerDataFrameAsTable(subpedidos_txt_Historia_df, "Zdc_Lotus_subpedidos_Tablaintermedia_txt_Historia")
    
    ValorY = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_LOTUS_PEDIDOS_APP' LIMIT 1").first()["Valor1"]
    
    
    panel_principal_subpedidos = sqlContext.sql ("\
    SELECT \
      UniversalID, \
      key_NomPedido, \
      UniversalIDPadre, \
      txt_Autor Autor, \
      txt_AsuntoPed, \
      dat_FchCreac, \
      IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/subpedidos/', UniversalID,'.zip'" + "), '') link, \
      translate(concat(concat(if(key_NomPedido is null, '',key_NomPedido), ' - ', \
                              if(txt_Autor is null, '',txt_Autor), ' - ', \
                              if(dat_FchCreac is null, '',dat_FchCreac), ' - ', \
                              if(txt_AsuntoPed is null, '',txt_AsuntoPed)), ' - ',  \
      upper(concat(if(key_NomPedido is null, '',key_NomPedido), ' - ', \
                   if(txt_Autor is null, '',txt_Autor), ' - ', \
                   if(txt_AsuntoPed is null, '',txt_AsuntoPed))), ' - ', \
      lower(concat(if(key_NomPedido is null, '',key_NomPedido), ' - ', \
                   if(txt_Autor is null, '',txt_Autor), ' - ', \
                   if(txt_AsuntoPed is null, '',txt_AsuntoPed)))),'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN')  AS busqueda, \
      anio, \
      mes \
    FROM \
      Zdc_Lotus_Subpedidos_TablaIntermedia")
    
    
    panel_detalle_subpedidos = sqlContext.sql("\
    SELECT UniversalId, 'Encabezado' AS vista, 1 as posicion, 'Autor' AS key, txt_Autor AS value, anio, mes FROM  Zdc_Lotus_Subpedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Encabezado' AS vista, 2 as posicion, 'Fecha Creación' AS key, dat_FchCreac AS value, anio, mes FROM  Zdc_Lotus_Subpedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 3 as posicion, 'Nombres' AS key, txt_NomCompEmp_A AS value, anio, mes FROM  Zdc_Lotus_Subpedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 4 as posicion, 'Dependencia' AS key, txt_DepEmp_A AS value, anio, mes FROM  Zdc_Lotus_Subpedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 5 as posicion, 'Región' AS key, txt_RgnEmp_A AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4)as int) AS Anio,  cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2)as int) AS Mes FROM  Zdc_Lotus_Subpedidos_Tablaintermedia_txt_RgnEmp_A \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 6 as posicion, 'Ubicación' AS key, txt_UbicEmp_A AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4)as int) AS Anio,  cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2)as int) AS Mes FROM  Zdc_Lotus_Subpedidos_Tablaintermedia_txt_UbicEmp_A \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 7 as posicion, 'Teléfono' AS key, txt_TelEnteExt AS value, anio, mes FROM  Zdc_Lotus_Subpedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 8 as posicion, 'Fecha de Entrega de Definición' AS key, dat_FchIniTram AS value, anio, mes FROM  Zdc_Lotus_Subpedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' AS vista, 9 as posicion, 'Tipo de Pedido' AS key, key_NomPedido AS value, anio, mes FROM  Zdc_Lotus_Subpedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' AS vista, 10 as posicion, 'Asunto' AS key, txt_AsuntoPed AS value, anio, mes FROM  Zdc_Lotus_Subpedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 11 as posicion, 'Estado' AS key, txt_Estado AS value, anio, mes FROM  Zdc_Lotus_Subpedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 12 as posicion, 'Fecha del Estado' AS key, dat_FchEstadoAct AS value, anio, mes FROM  Zdc_Lotus_Subpedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Asignación de Responsable' AS vista, 13 as posicion, 'Responsable' AS key, txt_Responsable AS value, anio, mes FROM  Zdc_Lotus_Subpedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 14 as posicion, 'Nombres' AS key, txt_NomCompEmp_R AS value, anio, mes FROM  Zdc_Lotus_Subpedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 15 as posicion, 'Empresa' AS key, txt_EmpresaEmp_R AS value, anio, mes FROM  Zdc_Lotus_Subpedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 16 as posicion, 'Dependencia' AS key, txt_DepEmp_R AS value, anio, mes FROM  Zdc_Lotus_Subpedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 17 as posicion, 'Región' AS key, txt_RgnEmp_R AS value, anio, mes FROM  Zdc_Lotus_Subpedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 18 as posicion, 'Ubicación' AS key, txt_UbicEmp_R AS value, anio, mes FROM  Zdc_Lotus_Subpedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 19 as posicion, 'Teléfono' AS key, txt_ExtEmp_R AS value, anio, mes FROM  Zdc_Lotus_Subpedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Comentarios' AS vista, 20 as posicion, '' AS key, txt_Comentarios AS value, anio, mes FROM  Zdc_Lotus_Subpedidos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Historia' AS vista, 21 as posicion, '' AS key, txt_Historia AS txt_, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4)as int) AS Anio,  cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2)as int) AS Mes FROM  Zdc_Lotus_subpedidos_Tablaintermedia_txt_Historia_array")
    
    return subpedidos_df, panel_principal_subpedidos, panel_detalle_subpedidos
  except Exception as Error:
    print(Error, "\n Error validando auditoria Subpedidos")

# COMMAND ----------

def ft_reclasificados(ar_path):
  try:
    reclasificados_df = spark.read.schema(ft_schema_reclasificados().schema).json(ar_path)
    reclasificados_df = reclasificados_df.select(col('UniversalID'),
                                                 col('attachments'),
                                                 col('txtTipoReclasificado.value').alias('txtTipoReclasificado'),
                                                 col('Dat_Fcreacion.value').alias('Dat_Fcreacion'),
                                                 col('Str_Autor.value').alias('Str_Autor'),
                                                 col('str_cliidentificacion.value').alias('str_cliidentificacion'),
                                                 col('str_cliTipIdentificacion.value').alias('str_cliTipIdentificacion'),
                                                 col('str_cliNombre.value').alias('str_cliNombre'),
                                                 col('str_cliGerente.value').alias('str_cliGerente'),
                                                 col('str_CodGerente.value').alias('str_CodGerente'),
                                                 col('Str_Region.value').alias('Str_Region'),
                                                 col('txtSucursal.value').alias('txtSucursal'),
                                                 col('txtCalificacionInternaCliente.value').alias('txtCalificacionInternaCliente'),
                                                 col('Str_Radicado.value').cast('int').alias('Str_Radicado'),
                                                 col('Dat_Fdesicion.value').alias('Dat_Fdesicion'),
                                                 col('Str_Reclasificador.value').alias('Str_Reclasificador'),
                                                 col('Str_Estado.value').alias('Str_Estado'),
                                                 col('str_Responsable.value').alias('str_Responsable'),
                                                 col('str_cliSegmento.value').alias('str_cliSegmento'),col('txtPuntajeCliente.value').alias('txtPuntajeCliente'),
                                                 col('Str_NumIdentificacion.value').alias('Str_NumIdentificacion'),array_join('Historia.value', '\r\n').alias('Historia'),
                                                 col('hijos.Doc1.value').getItem(0).alias('Doc1'),
                                                 col('hijos.Doc2.value').getItem(0).alias('Doc2'),
                                                 col('hijos.Doc3.value').getItem(0).alias('Doc3'),
                                                 col('hijos.Doc4.value').getItem(0).alias('Doc4'),
                                                 col('hijos.Doc5.value').getItem(0).alias('Doc5'),
                                                 col('hijos.Doc6.value').getItem(0).alias('Doc6'),
                                                 col('hijos.Doc7.value').getItem(0).alias('Doc7'),
                                                 col('hijos.Doc8.value').getItem(0).alias('Doc8'),
                                                 col('hijos.Doc9.value').getItem(0).alias('Doc9'),
                                                 col('hijos.Doc10.value').getItem(0).alias('Doc10'),
                                                 col('hijos.Doc11.value').getItem(0).alias('Doc11'),
                                                 col('hijos.Doc12.value').getItem(0).alias('Doc12'),
                                                 col('hijos.Doc13.value').getItem(0).alias('Doc13'),
                                                 col('hijos.Doc14.value').getItem(0).alias('Doc14'),
                                                 col('hijos.Doc15.value').getItem(0).alias('Doc15'),
                                                 col('hijos.Doc16.value').getItem(0).alias('Doc16'),
                                                 col('hijos.Doc17.value').getItem(0).alias('Doc17'),
                                                 col('hijos.Doc18.value').getItem(0).alias('Doc18'),
                                                 col('hijos.Doc19.value').getItem(0).alias('Doc19'),
                                                 col('hijos.ok1.value').getItem(0).alias('ok1'),
                                                 col('hijos.ok2.value').getItem(0).alias('ok2'),
                                                 col('hijos.ok3.value').getItem(0).alias('ok3'),
                                                 col('hijos.ok4.value').getItem(0).alias('ok4'),
                                                 col('hijos.ok5.value').getItem(0).alias('ok5'),
                                                 col('hijos.ok6.value').getItem(0).alias('ok6'),
                                                 col('hijos.ok7.value').getItem(0).alias('ok7'),
                                                 col('hijos.ok8.value').getItem(0).alias('ok8'),
                                                 col('hijos.ok9.value').getItem(0).alias('ok9'),
                                                 col('hijos.ok10.value').getItem(0).alias('ok10'),
                                                 col('hijos.ok11.value').getItem(0).alias('ok11'),
                                                 col('hijos.ok12.value').getItem(0).alias('ok12'),
                                                 col('hijos.ok13.value').getItem(0).alias('ok13'),
                                                 col('hijos.ok14.value').getItem(0).alias('ok14'),
                                                 col('hijos.ok15.value').getItem(0).alias('ok15'),
                                                 col('hijos.ok16.value').getItem(0).alias('ok16'),
                                                 col('hijos.ok17.value').getItem(0).alias('ok17'),
                                                 col('hijos.ok18.value').getItem(0).alias('ok18'),
                                                 col('hijos.ok19.value').getItem(0).alias('ok19'),
                                                 col('hijos.ok1_1.value').getItem(0).alias('ok1_1'),
                                                 col('hijos.ok2_1.value').getItem(0).alias('ok2_1'),
                                                 col('hijos.ok3_1.value').getItem(0).alias('ok3_1'),
                                                 col('hijos.ok4_1.value').getItem(0).alias('ok4_1'),
                                                 col('hijos.ok5_1.value').getItem(0).alias('ok5_1'),
                                                 col('hijos.ok6_1.value').getItem(0).alias('ok6_1'),
                                                 col('hijos.ok7_1.value').getItem(0).alias('ok7_1'),
                                                 col('hijos.ok8_1.value').getItem(0).alias('ok8_1'),
                                                 col('hijos.ok9_1.value').getItem(0).alias('ok9_1'),
                                                 col('hijos.ok10_1.value').getItem(0).alias('ok10_1'),
                                                 col('hijos.ok11_1.value').getItem(0).alias('ok11_1'),
                                                 col('hijos.ok12_1.value').getItem(0).alias('ok12_1'),
                                                 col('hijos.ok13_1.value').getItem(0).alias('ok13_1'),
                                                 col('hijos.ok14_1.value').getItem(0).alias('ok14_1'),
                                                 col('hijos.ok15_1.value').getItem(0).alias('ok15_1'),
                                                 col('hijos.ok16_1.value').getItem(0).alias('ok16_1'),
                                                 col('hijos.ok17_1.value').getItem(0).alias('ok17_1'),
                                                 col('hijos.ok18_1.value').getItem(0).alias('ok18_1'),
                                                 col('hijos.ok19_1.value').getItem(0).alias('ok19_1'))

    sqlContext.registerDataFrameAsTable(reclasificados_df, "Zdc_Lotus_Reclasificados_TablaIntermedia")

    ValorY = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_LOTUS_RECLASIFICADOS_APP' LIMIT 1").first()["Valor1"]
    
    sql_String = "SELECT \
        UniversalId \
        ,cast(Str_Radicado as int) Radicado \
        ,str_cliSegmento Segmento \
        ,str_cliGerente Gerente \
        ,str_cliNombre RazonSocial \
        ,str_cliidentificacion NumeroIdentificacion \
        ,Dat_Fcreacion FechaCreacion \
        ,translate(concat(concat(Str_Radicado,' ',str_cliSegmento,' ',str_cliGerente,' ',str_cliNombre,' ',str_cliidentificacion) \
        ,upper(concat(Str_Radicado,' ',str_cliSegmento,' ',str_cliGerente,' ',str_cliNombre,' ',str_cliidentificacion)) \
        ,lower(concat(Str_Radicado,' ',str_cliSegmento,' ',str_cliGerente,' ',str_cliNombre,' ',str_cliidentificacion))),'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN') busqueda \
        ,IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/', UniversalID,'.zip'" + "), '') link \
        ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio \
        ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes \
      FROM \
        Zdc_Lotus_Reclasificados_TablaIntermedia"
    panel_principal_reclasificados = sqlContext.sql(sql_String)

    panel_detalle_reclasificados = sqlContext.sql("\
      SELECT UniversalId, 'Datos Solicitud' AS vista, 1 as posicion, 'Fecha de Creacion' AS key, Dat_Fcreacion AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
        UNION ALL \
      SELECT UniversalId, 'Datos Solicitud' AS vista, 2 as posicion, 'Fecha de Decision Por' AS key, Dat_Fdesicion AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
        UNION ALL \
      SELECT UniversalId, 'Datos Solicitud' AS vista, 3 as posicion, 'Creado Por' AS key, Str_Autor AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
        UNION ALL \
      SELECT UniversalId, 'Datos Solicitud' AS vista, 4 as posicion, 'Reclasificador' AS key, Str_Reclasificador AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
        UNION ALL \
      SELECT UniversalId, 'Estado' AS vista, 5 as posicion, 'Estado Actual' AS key, Str_Estado AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
        UNION ALL \
      SELECT UniversalId, 'Estado' AS vista, 6 as posicion, 'Asignado A' AS key, str_Responsable AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
        UNION ALL \
      SELECT UniversalId, 'Informacion Cliente' AS vista, 7 as posicion, 'Numero de Identificacion' AS key, str_cliidentificacion AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
        UNION ALL \
      SELECT UniversalId, 'Informacion Cliente' AS vista, 8 as posicion, 'Tipo Identificacion' AS key, str_cliTipIdentificacion AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
        UNION ALL \
      SELECT UniversalId, 'Informacion Cliente' AS vista, 9 as posicion, 'Razon Social' AS key, str_cliNombre AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
        UNION ALL \
      SELECT UniversalId, 'Informacion Cliente' AS vista, 10 as posicion, 'Gerente' AS key, str_cliGerente AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
        UNION ALL \
      SELECT UniversalId, 'Informacion Cliente' AS vista, 11 as posicion, 'Codigo Oficial' AS key, str_CodGerente AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
        UNION ALL \
      SELECT UniversalId, 'Informacion Cliente' AS vista, 12 as posicion, 'Region' AS key, Str_Region AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
        UNION ALL \
      SELECT UniversalId, 'Informacion Cliente' AS vista, 13 as posicion, 'Sucursal' AS key, txtSucursal AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
        UNION ALL \
      SELECT UniversalId, 'Informacion Cliente' AS vista, 14 as posicion, 'Calificacion Interna Actual' AS key, txtCalificacionInternaCliente AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
        UNION ALL \
      SELECT UniversalId, 'Informacion Cliente' AS vista, 15 as posicion, 'Puntaje' AS key, txtPuntajeCliente AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
        UNION ALL \
      SELECT UniversalId, 'Registro Historico' AS vista, 16 as posicion, 'Registro Historico' AS key, historia AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia")

    return reclasificados_df, panel_principal_reclasificados, panel_detalle_reclasificados
  except Exception as Error:
    print(Error, "\nError validando auditoria reclasificados")

# COMMAND ----------

def ft_requerimientos(ar_path):
  try:
    req_legales_df = spark.read.schema(ft_schema_req_legales().schema).json(ar_path)
    req_legales_df = req_legales_df.select("UniversalID","attachments",
                                           col("dat_FchCreac.value").alias("dat_FchCreac"),
                                           col("dat_FchEstadoAct.value").alias("dat_FchEstadoAct"),
                                           col("dat_FchIniTram.value").alias("dat_FchIniTram"),
                                           col("dat_FchMaxEstado.value").alias("dat_FchMaxEstado"),
                                           col("dat_FchVencEstado.value").alias("dat_FchVencEstado"),
                                           col("dat_FechOficio.value").alias("dat_FechOficio"),
                                           col("dat_FechVto.value").alias("dat_FechVto"),
                                           col("key_Ciudad.value").alias("key_Ciudad"),
                                           col("key_EnteSol.value").alias("key_EnteSol"),
                                           col("key_Filial.value").alias("key_Filial"),
                                           col("key_NomPedido.value").alias("key_NomPedido"),
                                           col("key_SucArea.value").alias("key_SucArea"),
                                           col("key_TipoConteo.value").alias("key_TipoConteo"),
                                           col("key_TipoEnteExt.value").alias("key_TipoEnteExt"),
                                           col("key_TipoReqmto.value").alias("key_TipoReqmto"),
                                           col("key_ViceAtdPed.value").alias("key_ViceAtdPed"),
                                           col("num_DVParcial.value").alias("num_DVParcial"),
                                           col("num_DVTotal.value").alias("num_DVTotal"),
                                           col("num_DiaSol.value").alias("num_DiaSol"),
                                           col("num_DiasPro.value").alias("num_DiasPro"),
                                           col("num_TCotizado.value").alias("num_TCotizado"),
                                           col("num_TMaxEstado.value").alias("num_TMaxEstado"),
                                           col("txt_AsuntoPed.value").alias("txt_AsuntoPed"),
                                           col("txt_Autor.value").alias("txt_Autor"),
                                           col("txt_Banco.value").alias("txt_Banco"),
                                           col("txt_Creador.value").alias("txt_Creador"),
                                           col("txt_CtaDep.value").alias("txt_CtaDep"),
                                           col("txt_DepEmp_A.value").alias("txt_DepEmp_A"),
                                           col("txt_DepEmp_R.value").alias("txt_DepEmp_R"),
                                           col("txt_Direccion.value").alias("txt_Direccion"),
                                           col("txt_EmpEnteExt.value").alias("txt_EmpEnteExt"),
                                           col("txt_Estado.value").alias("txt_Estado"),
                                           col("txt_ExtEmp_A.value").alias("txt_ExtEmp_A"),
                                           col("txt_ExtEmp_R.value").alias("txt_ExtEmp_R"),
                                           col("txt_Funcionario.value").alias("txt_Funcionario"),
                                           col("txt_NomCompEmp_A.value").alias("txt_NomCompEmp_A"),
                                           col("txt_NomCompEmp_R.value").alias("txt_NomCompEmp_R"),
                                           col("txt_NumOficio.value").alias("txt_NumOficio"),
                                           col("txt_OtroEnte.value").alias("txt_OtroEnte"),
                                           col("txt_OtroReqmto.value").alias("txt_OtroReqmto"),
                                           col("txt_Pcto.value").alias("txt_Pcto"),
                                           col("txt_Responsable.value").alias("txt_Responsable"),
                                           col("txt_RgnEmp_A.value").alias("txt_RgnEmp_A"),
                                           col("txt_RgnEmp_R.value").alias("txt_RgnEmp_R"),
                                           col("txt_TelEnteExt.value").alias("txt_TelEnteExt"),
                                           col("txt_UbicEmp_A.value").alias("txt_UbicEmp_A"),
                                           col("txt_UbicEmp_R.value").alias("txt_UbicEmp_R"),
                                           array_join("txt_Historia.value", '\r\n ').alias("txt_Historia"))

    sqlContext.registerDataFrameAsTable(req_legales_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia")  
    
    """txt_CedNit como String"""
    req_legales_CedNit_String_payload = ["{'UniversalID':'string','txt_CedNit':{'label':'string', 'value':'string'}}"]

    sc = spark.sparkContext
    req_legales_CedNit_String_rdd = sc.parallelize(req_legales_CedNit_String_payload)
    req_legales_CedNit_String_json = spark.read.json(req_legales_CedNit_String_rdd)
    req_legales_CedNit_String_df = spark.read.schema(req_legales_CedNit_String_json.schema).json(ar_path)
    req_legales_CedNit_String_df = req_legales_CedNit_String_df.selectExpr("UniversalID","txt_CedNit.value as txt_CedNit")
    req_legales_CedNit_String_df = req_legales_CedNit_String_df.na.fill('')
    req_legales_CedNit_String_df = req_legales_CedNit_String_df.filter(req_legales_CedNit_String_df.txt_CedNit.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(req_legales_CedNit_String_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia_CedNit_string")
    
    """txt_CedNit como Array"""
    req_legales_CedNit_Array_payload = ["{'UniversalID':'string','txt_CedNit':{'label':'string', 'value':['array']}}"]
    sc = spark.sparkContext
    req_legales_CedNit_Array_rdd = sc.parallelize(req_legales_CedNit_Array_payload)
    req_legales_CedNit_Array_json = spark.read.json(req_legales_CedNit_Array_rdd)
    req_legales_CedNit_Array_df = spark.read.schema(req_legales_CedNit_Array_json.schema).json(ar_path)
    req_legales_CedNit_Array_df = req_legales_CedNit_Array_df.selectExpr("UniversalID","txt_CedNit.value as txt_CedNit")
    req_legales_CedNit_Array_df = req_legales_CedNit_Array_df.filter(col("UniversalID").isNotNull())
    req_legales_CedNit_Array_df = req_legales_CedNit_Array_df.select("UniversalID", array_join("txt_CedNit", '\r\n ').alias("txt_CedNit"))
    req_legales_CedNit_Array_df = req_legales_CedNit_Array_df.na.fill('')
    sqlContext.registerDataFrameAsTable(req_legales_CedNit_Array_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia_CedNit_array")
    
    req_legales_CedNit_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia_CedNit_array UNION SELECT * FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia_CedNit_string")
    sqlContext.registerDataFrameAsTable(req_legales_CedNit_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia_CedNit")

    """rch_DetallePed como String"""
    req_legales_DetallePed_String_payload = ["{'UniversalID':'string','rch_DetallePed':{'label':'string', 'value':'string'}}"]
    sc = spark.sparkContext
    req_legales_DetallePed_String_rdd = sc.parallelize(req_legales_DetallePed_String_payload)
    req_legales_DetallePed_String_json = spark.read.json(req_legales_DetallePed_String_rdd)
    req_legales_DetallePed_String_df = spark.read.schema(req_legales_DetallePed_String_json.schema).json(ar_path)
    req_legales_DetallePed_String_df = req_legales_DetallePed_String_df.selectExpr("UniversalID","rch_DetallePed.value as rch_DetallePed")
    req_legales_DetallePed_String_df = req_legales_DetallePed_String_df.na.fill('')
    req_legales_DetallePed_String_df = req_legales_DetallePed_String_df.filter(req_legales_DetallePed_String_df.rch_DetallePed.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(req_legales_DetallePed_String_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia_DetallePed_string")
    
    """rch_DetallePed como Array"""
    req_legales_DetallePed_Array_payload = ["{'UniversalID':'string','rch_DetallePed':{'label':'string', 'value':['array']}}"]
    sc = spark.sparkContext
    req_legales_DetallePed_Array_rdd = sc.parallelize(req_legales_DetallePed_Array_payload)
    req_legales_DetallePed_Array_json = spark.read.json(req_legales_DetallePed_Array_rdd)
    req_legales_DetallePed_Array_df = spark.read.schema(req_legales_DetallePed_Array_json.schema).json(ar_path)
    req_legales_DetallePed_Array_df = req_legales_DetallePed_Array_df.selectExpr("UniversalID","rch_DetallePed.value as rch_DetallePed")
    req_legales_DetallePed_Array_df = req_legales_DetallePed_Array_df.filter(col("UniversalID").isNotNull())
    req_legales_DetallePed_Array_df = req_legales_DetallePed_Array_df.select("UniversalID", array_join("rch_DetallePed", '\r\n ').alias("rch_DetallePed"))
    req_legales_DetallePed_Array_df = req_legales_DetallePed_Array_df.na.fill('')
    sqlContext.registerDataFrameAsTable(req_legales_DetallePed_Array_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia_DetallePed_array")
    
    req_legales_DetallePed_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia_DetallePed_array UNION SELECT * FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia_DetallePed_string")
    sqlContext.registerDataFrameAsTable(req_legales_DetallePed_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia_DetallePed")
    
    """txt_Comentarios como String"""
    req_legales_Comentarios_String_payload = ["{'UniversalID':'string','txt_Comentarios':{'label':'string', 'value':'string'}}"]
    sc = spark.sparkContext
    req_legales_Comentarios_String_rdd = sc.parallelize(req_legales_Comentarios_String_payload)
    req_legales_Comentarios_String_json = spark.read.json(req_legales_Comentarios_String_rdd)
    req_legales_Comentarios_String_df = spark.read.schema(req_legales_Comentarios_String_json.schema).json(ar_path)
    req_legales_Comentarios_String_df = req_legales_Comentarios_String_df.selectExpr("UniversalID","txt_Comentarios.value as txt_Comentarios")
    req_legales_Comentarios_String_df = req_legales_Comentarios_String_df.na.fill('')
    req_legales_Comentarios_String_df = req_legales_Comentarios_String_df.filter(req_legales_Comentarios_String_df.txt_Comentarios.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(req_legales_Comentarios_String_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia_Comentarios_string")

    """txt_Comentarios como Array"""
    req_legales_Comentarios_Array_payload = ["{'UniversalID':'string','txt_Comentarios':{'label':'string', 'value':['array']}}"]

    sc = spark.sparkContext
    req_legales_Comentarios_Array_rdd = sc.parallelize(req_legales_Comentarios_Array_payload)
    req_legales_Comentarios_Array_json = spark.read.json(req_legales_Comentarios_Array_rdd)
    req_legales_Comentarios_Array_df = spark.read.schema(req_legales_Comentarios_Array_json.schema).json(ar_path)
    req_legales_Comentarios_Array_df = req_legales_Comentarios_Array_df.selectExpr("UniversalID","txt_Comentarios.value as txt_Comentarios")
    req_legales_Comentarios_Array_df = req_legales_Comentarios_Array_df.filter(col("UniversalID").isNotNull())
    req_legales_Comentarios_Array_df = req_legales_Comentarios_Array_df.select("UniversalID", array_join("txt_Comentarios", '\r\n ').alias("txt_Comentarios"))
    req_legales_Comentarios_Array_df = req_legales_Comentarios_Array_df.na.fill('')
    sqlContext.registerDataFrameAsTable(req_legales_Comentarios_Array_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia_Comentarios_array")
    
    req_legales_Comentarios_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia_Comentarios_array UNION SELECT * FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia_Comentarios_string")
    sqlContext.registerDataFrameAsTable(req_legales_Comentarios_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia_Comentarios")
    
    ValorY = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_LOTUS_REQ_LEG_APP' LIMIT 1").first()["Valor1"]
    
    panel_principal_req_leg = sqlContext.sql("SELECT TI.UniversalId, \
      TI.txt_AsuntoPed AS Asunto, \
      TI.key_TipoReqmto AS TipoRequerimiento, \
      substring_index(TI.dat_FchCreac, ' ', 1) AS fecha_requerimiento_legal, \
      IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/', TI.UniversalID,'.zip'" + "), '') link, \
      translate(concat(concat(if(txt_AsuntoPed is null, '',txt_AsuntoPed), ' - ', \
                              if(key_TipoReqmto is null,'',key_TipoReqmto), ' - ', \
                              if(CN.txt_CedNit is null, '', txt_CedNit)), ' - ', \
      upper(concat(if(txt_AsuntoPed is null, '',txt_AsuntoPed), ' - ', \
                   if(key_TipoReqmto is null, '', key_TipoReqmto))), ' - ', \
      lower(concat(if(txt_AsuntoPed is null, '',txt_AsuntoPed), ' - ', \
                   if(key_TipoReqmto is null, '', key_TipoReqmto)))),'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN')  AS busqueda, \
      cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, \
      cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes \
    FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia AS TI \
      LEFT JOIN Zdc_Lotus_RequerimientosLegales_Tablaintermedia_CedNit AS CN ON TI.UniversalId = CN.UniversalId")

    panel_detalle_req_leg = sqlContext.sql("\
    SELECT UniversalId, 'Encabezado' AS vista, 1 as posicion, 'Autor' AS key, txt_Autor AS value, \
    cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM \
    Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Encabezado' AS vista, 2 as posicion, 'Creador' AS key, txt_Creador AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Encabezado' AS vista, 3 as posicion, 'Fecha creación' AS key, dat_FchCreac AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 4 as posicion, 'Nombres' AS key, txt_NomCompEmp_A AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 5 as posicion, 'Dependencia' AS key, txt_DepEmp_A AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 6 as posicion, 'Región' AS key, txt_RgnEmp_A AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 7 as posicion, 'Ubicación' AS key, txt_UbicEmp_A AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 8 as posicion, 'Teléfono' AS key, txt_ExtEmp_A AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 9 as posicion, 'Tipo Empresa' AS key, key_TipoEnteExt AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 10 as posicion, 'Nombre Empresa' AS key, txt_EmpEnteExt AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 11 as posicion, 'Ext Teléfono Empresa' AS key, txt_TelEnteExt AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Encabezado Datos Generales' AS vista, 12 as posicion, 'Fecha de entrega de definición' AS key, dat_FchIniTram AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Encabezado Datos Generales' AS vista, 13 as posicion, 'Vencido (días hábiles totales)' AS key, IF(num_DVTotal = '', '0', num_DVTotal) AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Encabezado Datos Generales' AS vista, 14 as posicion, 'Vencido (días hábiles estado actual)' AS key, IF(num_DVParcial = '', '0', num_DVParcial) AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' AS vista, 15 as posicion, '' AS key, key_ViceAtdPed AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' AS vista, 16 as posicion, 'Tipo Solicitud' AS key, key_NomPedido AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' AS vista, 17 as posicion, 'Asunto' AS key, txt_AsuntoPed AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 18 as posicion, 'Estado' AS key, txt_Estado AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 19 as posicion, 'Fecha del estado' AS key, dat_FchEstadoAct AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 20 as posicion, 'Tiempo máximo en el estado (días)' AS key, num_TMaxEstado AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 21 as posicion, 'Fecha máxima del estado' AS key, dat_FchMaxEstado AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 22 as posicion, 'Tiempo cotizado (días)' AS key, num_TCotizado AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 23 as posicion, 'Fecha vencimiento' AS key, dat_FchVencEstado AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Asignación de Responsable' AS vista, 24 as posicion, 'Responsable' AS key, txt_Responsable AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 25 as posicion, 'Nombres' AS key, txt_NomCompEmp_R AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 26 as posicion, 'Dependencia' AS key, txt_DepEmp_R AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 27 as posicion, 'Región' AS key, txt_RgnEmp_R AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 28 as posicion, 'Ubicación' AS key, txt_UbicEmp_R AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 29 as posicion, 'Teléfono' AS key, txt_ExtEmp_R AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT ti.UniversalId, 'Detalle del Pedido' AS vista, 30 as posicion, 'Detalle pedido' AS key, dp.rch_DetallePed AS value, substr(substring_index(ti.dat_FchCreac, ' ', 1),-4,4) AS anio, substr(substring_index(ti.dat_FchCreac, ' ', 1),-7,2) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia ti \
    INNER JOIN Zdc_Lotus_RequerimientosLegales_Tablaintermedia_DetallePed AS dp ON ti.UniversalId = dp.UniversalId \
    UNION ALL \
    SELECT UniversalId, 'Datos Origen Solicitud' AS vista, 31 as posicion, 'Filial' AS key, key_Filial AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Origen Solicitud' AS vista, 32 as posicion, 'Sucursal o Área origen' AS key, key_SucArea AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Origen Solicitud' AS vista, 33 as posicion, 'Ente solicitante' AS key, key_EnteSol AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Origen Solicitud' AS vista, 34 as posicion, 'Nombre del Ente' AS key, txt_OtroEnte AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Requerimiento' AS vista, 35 as posicion, 'Número del oficio' AS key, txt_NumOficio AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Requerimiento' AS vista, 36 as posicion, 'Fecha del oficio' AS key, dat_FechOficio AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Requerimiento' AS vista, 37 as posicion, 'Tipo de requerimiento' AS key, key_TipoReqmto AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Requerimiento' AS vista, 38 as posicion, 'Requerimiento' AS key, txt_OtroReqmto AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Requerimiento' AS vista, 39 as posicion, 'Días para su solución' AS key, concat(num_DiaSol, ' - ', CASE \
         WHEN UPPER(key_TipoConteo) = 'C' THEN 'Calendario' \
         WHEN UPPER(key_TipoConteo) = 'H' THEN 'Hábiles' \
         ELSE '' \
        END) AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Requerimiento' AS vista, 40 as posicion, 'Días de prórroga' AS key, num_DiasPro AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Requerimiento' AS vista, 41 as posicion, 'Fecha de vencimiento del oficio' AS key, dat_FechVto AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT ti.UniversalId, 'Datos Detalle' AS vista, 42 as posicion, 'Cédula o Nit' AS key, cn.txt_CedNit AS value, substr(substring_index(ti.dat_FchCreac, ' ', 1),-4,4) AS anio, substr(substring_index(ti.dat_FchCreac, ' ', 1),-7,2) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia ti \
    INNER JOIN Zdc_Lotus_RequerimientosLegales_Tablaintermedia_CedNit AS cn ON ti.UniversalId = cn.UniversalId \
    UNION ALL \
    SELECT UniversalId, 'Datos Detalle' AS vista, 43 as posicion, '¿Vinculado?' AS key, CASE \
         WHEN UPPER(txt_Pcto) = 'S' THEN 'Si' \
         WHEN UPPER(txt_Pcto) = 'N' THEN 'No' \
         ELSE '' \
        END AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Entidad' AS vista, 44 as posicion, 'Ciudad' AS key, key_Ciudad AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Entidad' AS vista, 45 as posicion, 'Funcionario' AS key, txt_Funcionario AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Entidad' AS vista, 46 as posicion, 'Dirección' AS key, txt_Direccion AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Entidad' AS vista, 47 as posicion, 'Banco' AS key, txt_Banco AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Entidad' AS vista, 48 as posicion, 'Cuenta déposito' AS key, txt_CtaDep AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
    UNION ALL \
    SELECT ti.UniversalId, 'Comentarios' AS vista, 49 as posicion, 'Comentarios' AS key, c.txt_Comentarios AS value, substr(substring_index(ti.dat_FchCreac, ' ', 1),-4,4) AS anio, substr(substring_index(ti.dat_FchCreac, ' ', 1),-7,2) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia ti \
    INNER JOIN Zdc_Lotus_RequerimientosLegales_Tablaintermedia_Comentarios AS c ON ti.UniversalId = c.UniversalId \
    UNION ALL \
    SELECT UniversalId, 'Historia' AS vista, 50 as posicion, 'Historia' AS key, concat_ws('\n', txt_Historia) AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia ")

    return req_legales_df, panel_principal_req_leg, panel_detalle_req_leg
  except Exception as Error:
    print(Error, "\nError validando auditoria requerimientos legales")

# COMMAND ----------

def ft_solicitudes(ar_path):
  try:
    solicitudes_df = spark.read.schema(ft_schema_solicitudes().schema).json(ar_path)
    solicitudes_df = solicitudes_df.select("UniversalID","attachments",
                                           col("txt_Autor.value").alias("txt_Autor"),
                                           col("dat_FchCreac.value").alias("dat_FchCreac"),
                                           col("txt_AsuntoPed.value").alias("txt_AsuntoPed"),
                                           col("key_ViceAtdPed.value").alias("key_ViceAtdPed"),
                                           col("key_NomPedido.value").alias("key_NomPedido"),
                                           col("txt_Creador.value").alias("txt_Creador"),
                                           col("key_TipoEnteExt.value").alias("key_TipoEnteExt"),
                                           col("dat_FchMaxEstado.value").alias("dat_FchMaxEstado"),
                                           col("txt_Comentarios.value").alias("txt_Comentarios"),
                                           col("rch_AnexosPed.value").alias("rch_AnexosPed"),
                                           col("rch_AnexosAdic.value").alias("rch_AnexosAdic"),
                                           col("txt_Titulo1.value").alias("txt_Titulo1"),
                                           col("txt_Titulo2.value").alias("txt_Titulo2"),
                                           col("txt_NomCompEmp_A.value").alias("txt_NomCompEmp_A"),
                                           col("txt_DepEmp_A.value").alias("txt_DepEmp_A"),
                                           col("txt_RgnEmp_A.value").alias("txt_RgnEmp_A"),
                                           col("txt_ExtEmp_A.value").alias("txt_ExtEmp_A"),
                                           col("txt_EmpEnteExt.value").alias("txt_EmpEnteExt"),
                                           col("txt_TelEnteExt.value").alias("txt_TelEnteExt"),
                                           col("dat_FchIniTram.value").alias("dat_FchIniTram"),
                                           col("num_DVTotal.value").alias("num_DVTotal"),
                                           col("num_DVParcial.value").alias("num_DVParcial"),
                                           col("txt_Estado.value").alias("txt_Estado"),
                                           col("dat_FchEstadoAct.value").alias("dat_FchEstadoAct"),
                                           col("num_TMaxEstado.value").alias("num_TMaxEstado"),
                                           col("dat_FchMaxEstado_1.value").alias("dat_FchMaxEstado_1"),
                                           col("num_TCotizado.value").alias("num_TCotizado"),
                                           col("num_TCotizado_1.value").alias("num_TCotizado_1"),
                                           col("num_PesosCotiz.value").alias("num_PesosCotiz"),
                                           col("num_PesosCotiz_1.value").alias("num_PesosCotiz_1"),
                                           col("dat_FchVencEstado.value").alias("dat_FchVencEstado"),
                                           col("txt_Responsable.value").alias("txt_Responsable"),
                                           col("txt_Responsable_1.value").alias("txt_Responsable_1"),
                                           col("txt_NomCompEmp_R.value").alias("txt_NomCompEmp_R"),
                                           col("txt_DepEmp_R.value").alias("txt_DepEmp_R"),
                                           col("txt_RgnEmp_R.value").alias("txt_RgnEmp_R"),
                                           col("txt_ExtEmp_R.value").alias("txt_ExtEmp_R"),
                                           col("txt_UbicEmp_A.value").alias("txt_UbicEmp_A"),
                                           col("txt_UbicEmp_R.value").alias("txt_UbicEmp_R"),
                                           col("rch_DetallePed.value").alias("rch_DetallePed"),
                                           substring(substring_index('dat_FchCreac.value', ' ', 1),-4,4).cast('int').alias('anio'),
                                           substring(substring_index('dat_FchCreac.value', ' ', 1),-7,2).cast('int').alias('mes'))

    sqlContext.registerDataFrameAsTable(solicitudes_df, "Zdc_Lotus_Solicitudes_TablaIntermedia")
    
    """txt_Historia como String"""
    solicitudes_txt_Historia_string_payload =  ["{'UniversalID':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_Historia':{'label':'string','value':'string'}}"]

    sc = spark.sparkContext
    solicitudes_txt_Historia_string_rdd = sc.parallelize(solicitudes_txt_Historia_string_payload)
    solicitudes_txt_Historia_string_json = spark.read.json(solicitudes_txt_Historia_string_rdd)
    solicitudes_txt_Historia_string_df = spark.read.schema(solicitudes_txt_Historia_string_json.schema).json(ar_path)
    solicitudes_txt_Historia_string_df = solicitudes_txt_Historia_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
    solicitudes_txt_Historia_string_df = solicitudes_txt_Historia_string_df.filter(solicitudes_txt_Historia_string_df.txt_Historia.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(solicitudes_txt_Historia_string_df, "Zdc_Lotus_Solicitudes_Tablaintermedia_txt_Historia_string")
    
    """txt_Historia como Array"""
    solicitudes_txt_Historia_array_payload = ["{'UniversalID':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_Historia':{'Label':'string','value':['array']}}"]

    sc = spark.sparkContext
    solicitudes_txt_Historia_array_rdd = sc.parallelize(solicitudes_txt_Historia_array_payload)
    solicitudes_txt_Historia_array_json = spark.read.json(solicitudes_txt_Historia_array_rdd)
    solicitudes_txt_Historia_array_df = spark.read.schema(solicitudes_txt_Historia_array_json.schema).json(ar_path)
    solicitudes_txt_Historia_array_df = solicitudes_txt_Historia_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
    solicitudes_txt_Historia_array_df = solicitudes_txt_Historia_array_df.filter(col("UniversalID").isNotNull())
    solicitudes_txt_Historia_array_df = solicitudes_txt_Historia_array_df.select("UniversalID","dat_FchCreac", array_join("txt_Historia", '\r\n ').alias("txt_Historia"))
    sqlContext.registerDataFrameAsTable(solicitudes_txt_Historia_array_df, "Zdc_Lotus_Solicitudes_Tablaintermedia_txt_Historia_array")
    
    solicitudes_txt_Historia_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Solicitudes_Tablaintermedia_txt_Historia_string UNION SELECT * FROM Zdc_Lotus_Solicitudes_Tablaintermedia_txt_Historia_array")
    sqlContext.registerDataFrameAsTable(solicitudes_txt_Historia_df, "Zdc_Lotus_Solicitudes_Tablaintermedia_txt_Historia")
    
    ValorY = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_LOTUS_SOL_CLI_APP' LIMIT 1").first()["Valor1"]
    
    panel_principal_solicitudes = sqlContext.sql ("\
    SELECT \
      UniversalID, \
      key_NomPedido, \
      txt_Autor Autor, \
      dat_FchCreac, \
      txt_AsuntoPed, \
      IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/', UniversalID,'.zip'" + "), '') link, \
      translate(concat(concat(if(txt_Autor is null, '',txt_Autor), ' - ', \
                              if(key_NomPedido is null, '',key_NomPedido), ' - ', \
                              if(txt_AsuntoPed is null, '', txt_AsuntoPed)), ' - ',  \
      upper(concat(if(txt_Autor is null, '',txt_Autor), ' - ', \
                   if(key_NomPedido is null, '',key_NomPedido), ' - ', \
                   if(txt_AsuntoPed is null, '', txt_AsuntoPed))), ' - ', \
      lower(concat(if(txt_Autor is null, '',txt_Autor), ' - ', \
                   if(key_NomPedido is null, '',key_NomPedido), ' - ', \
                   if(txt_AsuntoPed is null, '', txt_AsuntoPed)))),'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN')  AS busqueda, \
      anio, \
      mes \
    FROM \
      Zdc_Lotus_Solicitudes_TablaIntermedia")

    panel_detalle_solicitudes = sqlContext.sql("\
    SELECT UniversalId, 'Encabezado' AS vista, 1 as posicion, 'Autor' AS key, txt_Autor AS value, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Encabezado' AS vista, 2 as posicion, 'Fecha Creación' AS key, dat_FchCreac AS value, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Solicitantes' AS vista, 3 as posicion, 'Nombres' AS key, txt_NomCompEmp_A, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Solicitantes' AS vista, 4 as posicion, 'Dependencia' AS key, txt_DepEmp_A, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Solicitantes' AS vista, 5 as posicion, 'Región' AS key, txt_RgnEmp_A, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Solicitantes' AS vista, 6 as posicion, 'Ubicación' AS key, txt_UbicEmp_A, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado Solicitantes' AS vista, 7 as posicion, 'teléfono' AS key, txt_ExtEmp_A, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'datos Generales' AS vista, 8 as posicion, 'Tipo Solicitud' AS key, key_NomPedido, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'datos Generales' AS vista, 9 as posicion, 'Asunto' AS key, txt_AsuntoPed, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 10 as posicion, 'Estado' AS key, txt_Estado, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 11 as posicion, 'Fecha del estado' AS key, dat_FchEstadoAct, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado responsable' AS vista, 12 as posicion, 'Asignación de Responsable' AS key, txt_Responsable, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado responsable' AS vista, 14 as posicion, 'Nombres' AS key, txt_NomCompEmp_R, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado responsable' AS vista, 15 as posicion, 'Dependencia' AS key, txt_DepEmp_R, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado responsable' AS vista, 16 as posicion, 'Región' AS key, txt_RgnEmp_R, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado responsable' AS vista, 17 as posicion, 'Ubicación' AS key, txt_UbicEmp_R, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Información del Empleado responsable' AS vista, 18 as posicion, 'Teléfono' AS key, txt_ExtEmp_R, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Detalle del Pedido' AS vista, 19 as posicion, '' AS key, rch_DetallePed, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Comentarios' AS vista, 20 as posicion, '' AS key, txt_Comentarios, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Historia' AS vista, 21 as posicion, '' AS key, txt_Historia, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4)as int) AS Anio,  cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2)as int) AS Mes  FROM Zdc_Lotus_Solicitudes_Tablaintermedia_txt_Historia")
    
    return solicitudes_df, panel_principal_solicitudes, panel_detalle_solicitudes
  except Exception as Error:
    print(Error, "\nError validando auditoria solicitudes")

# COMMAND ----------

def ft_primercontacto(ar_path):
  try:
    primercontacto_df = spark.read.schema(ft_schema_primercontacto().schema).json(ar_path)
    sqlContext.registerDataFrameAsTable(primercontacto_df, "Zdc_Lotus_PrimerContacto_TablaIntermedia")
    
    """txthistoria como String"""
    primercontacto_txthistoria_string_payload =  ["{'UniversalID':'string','txthistoria':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]

    sc = spark.sparkContext
    primercontacto_txthistoria_string_rdd = sc.parallelize(primercontacto_txthistoria_string_payload)
    primercontacto_txthistoria_string_json = spark.read.json(primercontacto_txthistoria_string_rdd)
    primercontacto_txthistoria_string_df = spark.read.schema(primercontacto_txthistoria_string_json.schema).json(ar_path)
    primercontacto_txthistoria_string_df = primercontacto_txthistoria_string_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","txthistoria.value as txthistoria")
    primercontacto_txthistoria_string_df = primercontacto_txthistoria_string_df.filter(primercontacto_txthistoria_string_df.txthistoria.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(primercontacto_txthistoria_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_txthistoria_string")
    
    """txthistoria como String"""
    primercontacto_txthistoria_array_payload = ["{'UniversalID':'string','txthistoria':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}}"]

    sc = spark.sparkContext
    primercontacto_txthistoria_array_rdd = sc.parallelize(primercontacto_txthistoria_array_payload)
    primercontacto_txthistoria_array_json = spark.read.json(primercontacto_txthistoria_array_rdd)
    primercontacto_txthistoria_array_df = spark.read.schema(primercontacto_txthistoria_array_json.schema).json(ar_path)
    primercontacto_txthistoria_array_df = primercontacto_txthistoria_array_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","txthistoria.value as txthistoria")
    primercontacto_txthistoria_array_df = primercontacto_txthistoria_array_df.filter(col("UniversalID").isNotNull())
    primercontacto_txthistoria_array_df = primercontacto_txthistoria_array_df.select("UniversalID","dtFechaCreacion",array_join("txthistoria", '\r\n ').alias("txthistoria"))
    sqlContext.registerDataFrameAsTable(primercontacto_txthistoria_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_txthistoria_array")
    
    primercontacto_txthistoria_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_txthistoria_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_txthistoria_array")
    sqlContext.registerDataFrameAsTable(primercontacto_txthistoria_df, "Zdc_Lotus_primercontacto_Tablaintermedia_txthistoria")
    
    """txtComentario como String"""
    primercontacto_txtComentario_string_payload =  ["{'UniversalID':'string','txtComentario':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    primercontacto_txtComentario_string_rdd = sc.parallelize(primercontacto_txtComentario_string_payload)
    primercontacto_txtComentario_string_json = spark.read.json(primercontacto_txtComentario_string_rdd)
    primercontacto_txtComentario_string_df = spark.read.schema(primercontacto_txtComentario_string_json.schema).json(ar_path)
    primercontacto_txtComentario_string_df = primercontacto_txtComentario_string_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","txtComentario.value as txtComentario")
    primercontacto_txtComentario_string_df = primercontacto_txtComentario_string_df.filter(primercontacto_txtComentario_string_df.txtComentario.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(primercontacto_txtComentario_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_txtComentario_string")
    
    """txtComentario como String"""
    primercontacto_txtComentario_array_payload = ["{'UniversalID':'string','txtComentario':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}}"]
    sc = spark.sparkContext
    primercontacto_txtComentario_array_rdd = sc.parallelize(primercontacto_txtComentario_array_payload)
    primercontacto_txtComentario_array_json = spark.read.json(primercontacto_txtComentario_array_rdd)
    primercontacto_txtComentario_array_df = spark.read.schema(primercontacto_txtComentario_array_json.schema).json(ar_path)
    primercontacto_txtComentario_array_df = primercontacto_txtComentario_array_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","txtComentario.value as txtComentario")
    primercontacto_txtComentario_array_df = primercontacto_txtComentario_array_df.filter(col("UniversalID").isNotNull())
    primercontacto_txtComentario_array_df = primercontacto_txtComentario_array_df.select("UniversalID","dtFechaCreacion",array_join("txtComentario", '\r\n ').alias("txtComentario"))
    sqlContext.registerDataFrameAsTable(primercontacto_txtComentario_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_txtComentario_array")
    
    primercontacto_txtComentario_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_txtComentario_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_txtComentario_array")
    sqlContext.registerDataFrameAsTable(primercontacto_txtComentario_df, "Zdc_Lotus_primercontacto_Tablaintermedia_txtComentario")
    
    ValorY = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_LOTUS_RECLAMOS_APP' LIMIT 1").first()["Valor1"]
    
    panel_principal_primercontacto = sqlContext.sql("\
    SELECT \
      UniversalID, \
      dtFechaCreacion.value FechaSolucionado, \
      substring_index(dtFechaCreacion.value, ' ', 1) fecha_solicitudes, \
      cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4)as int) anio, \
      cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes, \
      txtNumIdCliente.value AS NumeroIdentificacion, \
      txtRadicado.value Radicado, \
      cbResponsable.value Responsable, \
      txtEstado.value Estado, \
      txtTipoProducto.value TipoProducto, \
      txtTipoReclamo.value TipoReclamo, \
      IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/primercontacto/', UniversalID,'.zip'" + "), '') link, \
      translate(concat(concat(txtNumIdCliente.value, ' - ', cbResponsable.value, ' - ', txtRadicado.value, ' - ', txtEstado.value, ' - ', txtTipoProducto.value, ' - ', txtTipoReclamo.value), ' - ', \
      upper(concat(txtNumIdCliente.value, ' - ', cbResponsable.value, ' - ', txtRadicado.value, ' - ', txtEstado.value, ' - ', txtTipoProducto.value, ' - ', txtTipoReclamo.value)), ' - ',  \
      lower(concat(txtNumIdCliente.value, ' - ', cbResponsable.value, ' - ', txtRadicado.value, ' - ', txtEstado.value, ' - ', txtTipoProducto.value, ' - ', txtTipoReclamo.value))),'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN') busqueda \
    FROM \
      Zdc_Lotus_PrimerContacto_TablaIntermedia")
    
    panel_detalle_primercontacto = sqlContext.sql("\
    SELECT UniversalId, 'Datos Generales' as vista, 1 as posicion, 'Autor' AS key, txtAutor.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' as vista, 2 as posicion, 'Fecha Solucionado Primer Contacto' AS key, dtFechaCreacion.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' as vista, 3 as posicion, 'Sucursal Captura' AS key, TXTSUCCAPTURA.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' as vista, 4 as posicion, 'Tipo Producto' AS key, txtTipoProducto.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' as vista, 5 as posicion, 'Tipo Reclamo' AS key, txtTipoReclamo.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' as vista, 6 as posicion, 'Numero Identificacion' AS key, txtNumIdCliente.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' as vista, 7 as posicion, 'Responsable' AS key, cbResponsable.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' as vista, 8 as posicion, 'Codigo Causalidad' AS key, txtCodCausalidad.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' as vista, 9 as posicion, 'Radicado' AS key, txtRadicado.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' as vista, 10 as posicion, 'Estado' AS key, txtEstado.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' as vista, 11 as posicion, 'Código Sucursal Captura' AS key, cbSucCaptura.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Historia' as vista, 12 as posicion, 'Historia' AS key, txthistoria AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_primercontacto_Tablaintermedia_txthistoria \
    UNION ALL \
    SELECT UniversalId, 'Comentario' as vista, 13 as posicion, 'Comentarios' AS key, txtComentario AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_primercontacto_Tablaintermedia_txtComentario")

    return primercontacto_df, panel_principal_primercontacto, panel_detalle_primercontacto
  except Exception as Error:
    print(Error, "\nError validando auditoria primer contacto diseño nuevo")

# COMMAND ----------

def ft_primercontacto_old(ar_path):
  try:
    primercontacto_old_df = spark.read.schema(ft_schema_primercontacto_old().schema).json(ar_path)
    sqlContext.registerDataFrameAsTable(primercontacto_old_df, "Zdc_Lotus_PrimerContacto_Old_TablaIntermedia")
    
    """txt_Comentarios como String"""
    primercontacto_old_txt_Comentarios_string_payload =  ["{'UniversalID':'string','txt_Comentarios':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    primercontacto_old_txt_Comentarios_string_rdd = sc.parallelize(primercontacto_old_txt_Comentarios_string_payload)
    primercontacto_old_txt_Comentarios_string_json = spark.read.json(primercontacto_old_txt_Comentarios_string_rdd)
    primercontacto_old_txt_Comentarios_string_df = spark.read.schema(primercontacto_old_txt_Comentarios_string_json.schema).json(ar_path)
    primercontacto_old_txt_Comentarios_string_df = primercontacto_old_txt_Comentarios_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Comentarios.value as txt_Comentarios")
    primercontacto_old_txt_Comentarios_string_df = primercontacto_old_txt_Comentarios_string_df.filter(primercontacto_old_txt_Comentarios_string_df.txt_Comentarios.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(primercontacto_old_txt_Comentarios_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_old_txt_Comentarios_string")

    """txt_Comentarios como String"""
    primercontacto_old_txt_Comentarios_array_payload = ["{'UniversalID':'string','txt_Comentarios':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}}"]
    sc = spark.sparkContext
    primercontacto_old_txt_Comentarios_array_rdd = sc.parallelize(primercontacto_old_txt_Comentarios_array_payload)
    primercontacto_old_txt_Comentarios_array_json = spark.read.json(primercontacto_old_txt_Comentarios_array_rdd)
    primercontacto_old_txt_Comentarios_array_df = spark.read.schema(primercontacto_old_txt_Comentarios_array_json.schema).json(ar_path)
    primercontacto_old_txt_Comentarios_array_df = primercontacto_old_txt_Comentarios_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Comentarios.value as txt_Comentarios")
    primercontacto_old_txt_Comentarios_array_df = primercontacto_old_txt_Comentarios_array_df.filter(col("UniversalID").isNotNull())
    primercontacto_old_txt_Comentarios_array_df = primercontacto_old_txt_Comentarios_array_df.select("UniversalID","dat_FchCreac",array_join("txt_Comentarios", '\r\n ').alias("txt_Comentarios"))
    sqlContext.registerDataFrameAsTable(primercontacto_old_txt_Comentarios_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_old_txt_Comentarios_array")
    
    primercontacto_old_txt_Comentarios_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_old_txt_Comentarios_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_old_txt_Comentarios_array")
    sqlContext.registerDataFrameAsTable(primercontacto_old_txt_Comentarios_df, "Zdc_Lotus_PrimerContacto_Old_Tablaintermedia_txt_Comentarios")
    
    """txt_Historia como String"""
    primercontacto_old_txt_Historia_string_payload =  ["{'UniversalID':'string','txt_Historia':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]

    sc = spark.sparkContext
    primercontacto_old_txt_Historia_string_rdd = sc.parallelize(primercontacto_old_txt_Historia_string_payload)
    primercontacto_old_txt_Historia_string_json = spark.read.json(primercontacto_old_txt_Historia_string_rdd)
    primercontacto_old_txt_Historia_string_df = spark.read.schema(primercontacto_old_txt_Historia_string_json.schema).json(ar_path)
    primercontacto_old_txt_Historia_string_df = primercontacto_old_txt_Historia_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
    primercontacto_old_txt_Historia_string_df = primercontacto_old_txt_Historia_string_df.filter(primercontacto_old_txt_Historia_string_df.txt_Historia.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(primercontacto_old_txt_Historia_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_old_txt_Historia_string")
    
    """txt_Historia como String"""
    primercontacto_old_txt_Historia_array_payload = ["{'UniversalID':'string','txt_Historia':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}}"]

    sc = spark.sparkContext
    primercontacto_old_txt_Historia_array_rdd = sc.parallelize(primercontacto_old_txt_Historia_array_payload)
    primercontacto_old_txt_Historia_array_json = spark.read.json(primercontacto_old_txt_Historia_array_rdd)
    primercontacto_old_txt_Historia_array_df = spark.read.schema(primercontacto_old_txt_Historia_array_json.schema).json(ar_path)
    primercontacto_old_txt_Historia_array_df = primercontacto_old_txt_Historia_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
    primercontacto_old_txt_Historia_array_df = primercontacto_old_txt_Historia_array_df.filter(col("UniversalID").isNotNull())
    primercontacto_old_txt_Historia_array_df = primercontacto_old_txt_Historia_array_df.select("UniversalID","dat_FchCreac",array_join("txt_Historia", '\r\n ').alias("txt_Historia"))
    sqlContext.registerDataFrameAsTable(primercontacto_old_txt_Historia_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_old_txt_Historia_array")
    
    primercontacto_old_txt_Historia_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_old_txt_Historia_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_old_txt_Historia_array")
    sqlContext.registerDataFrameAsTable(primercontacto_old_txt_Historia_df, "Zdc_Lotus_PrimerContacto_Old_Tablaintermedia_txt_Historia")
    
    ValorY = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_LOTUS_RECLAMOS_APP' LIMIT 1").first()["Valor1"]

    sql_string = "SELECT \
      UniversalID, \
      txt_CedNit.value CedNit, \
      substring_index(dat_FchCreac.value, ' ', 1) fecha_solicitudes, \
      cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4)as int) anio, \
      cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2)as int) mes, \
      key_Estado.value Estado, \
      nom_Responsable.value Responsable, \
      key_Producto.value Producto, \
      key_TipoReclamo.value TipoReclamo, \
      IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/primercontacto-old/', UniversalID,'.zip'" + "), '') link, \
      translate(concat(concat(txt_CedNit.value, ' - ', key_Estado.value, ' - ', key_Producto.value, ' - ', key_TipoReclamo.value), ' - ', \
      upper(concat(txt_CedNit.value, ' - ', key_Estado.value, ' - ', key_Producto.value, ' - ', key_TipoReclamo.value)), ' - ', \
      lower(concat(txt_CedNit.value, ' - ', key_Estado.value, ' - ', key_Producto.value, ' - ', key_TipoReclamo.value))),'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN') Busqueda \
    FROM \
      Zdc_Lotus_PrimerContacto_Old_TablaIntermedia"
    panel_principal_primercontacto_old = sqlContext.sql(sql_string)
    
    panel_detalle_primercontacto_old = sqlContext.sql("\
    SELECT UniversalId, '' AS vista, 1 as posicion, 'Estado' AS key, key_Estado.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_Old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' AS vista, 2 as posicion, 'Cédula/Nit' AS key, txt_CedNit.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_Old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' AS vista, 3 as posicion, 'Producto' AS key, key_Producto.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_Old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' AS vista, 4 as posicion, 'Tipo de Reclamo' AS key, key_TipoReclamo.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_Old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' AS vista, 5 as posicion, 'Responsable' AS key, nom_Responsable.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_Old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos Generales' AS vista, 6 as posicion, 'Código de Causalidad' AS key, txt_CodCausalidad.value  AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_Old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Comentarios' AS vista, 7 as posicion, ' ' AS key, txt_Comentarios AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_Old_Tablaintermedia_txt_Comentarios \
    UNION ALL \
    SELECT UniversalId, 'Historia' AS vista, 8 as posicion, ' ' AS key, txt_Historia AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_Old_Tablaintermedia_txt_Historia")
    
    return primercontacto_old_df, panel_principal_primercontacto_old, panel_detalle_primercontacto_old
  except Exception as Error:
    print(Error, "\nError validando auditoria primer contacto diseño viejo")

# COMMAND ----------

def ft_subreclamos(ar_path):
  try:
    subreclamos_df = spark.read.schema(ft_schema_subreclamos().schema).json(ar_path)
    sqlContext.registerDataFrameAsTable(subreclamos_df, "Zdc_Lotus_SubReclamos_TablaIntermedia")

    """cbTarjetas como String"""
    subreclamos_cbTarjetas_string_payload =  ["{'UniversalID':'string','cbTarjetas':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]

    sc = spark.sparkContext
    subreclamos_cbTarjetas_string_rdd = sc.parallelize(subreclamos_cbTarjetas_string_payload)
    subreclamos_cbTarjetas_string_json = spark.read.json(subreclamos_cbTarjetas_string_rdd)
    subreclamos_cbTarjetas_string_df = spark.read.schema(subreclamos_cbTarjetas_string_json.schema).json(ar_path)
    subreclamos_cbTarjetas_string_df = subreclamos_cbTarjetas_string_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","cbTarjetas.value as cbTarjetas")
    subreclamos_cbTarjetas_string_df = subreclamos_cbTarjetas_string_df.filter(subreclamos_cbTarjetas_string_df.cbTarjetas.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(subreclamos_cbTarjetas_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_cbTarjetas_string")
    
    """cbTarjetas como String"""
    subreclamos_cbTarjetas_array_payload = ["{'UniversalID':'string','cbTarjetas':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}}"]
    sc = spark.sparkContext
    subreclamos_cbTarjetas_array_rdd = sc.parallelize(subreclamos_cbTarjetas_array_payload)
    subreclamos_cbTarjetas_array_json = spark.read.json(subreclamos_cbTarjetas_array_rdd)
    subreclamos_cbTarjetas_array_df = spark.read.schema(subreclamos_cbTarjetas_array_json.schema).json(ar_path)
    subreclamos_cbTarjetas_array_df = subreclamos_cbTarjetas_array_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","cbTarjetas.value as cbTarjetas")
    subreclamos_cbTarjetas_array_df = subreclamos_cbTarjetas_array_df.filter(col("UniversalID").isNotNull())
    subreclamos_cbTarjetas_array_df = subreclamos_cbTarjetas_array_df.select("UniversalID","dtFechaCreacion",array_join("cbTarjetas", '\r\n ').alias("cbTarjetas"))
    sqlContext.registerDataFrameAsTable(subreclamos_cbTarjetas_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_cbTarjetas_array")
    
    subreclamo_cbTarjetas_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_cbTarjetas_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_cbTarjetas_array")
    sqlContext.registerDataFrameAsTable(subreclamo_cbTarjetas_df, "Zdc_Lotus_subreclamos_Tablaintermedia_cbTarjetas")
    
    """txtComentario como String"""
    subreclamos_txtComentario_string_payload =  ["{'UniversalID':'string','txtComentario':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    subreclamos_txtComentario_string_rdd = sc.parallelize(subreclamos_txtComentario_string_payload)
    subreclamos_txtComentario_string_json = spark.read.json(subreclamos_txtComentario_string_rdd)
    subreclamos_txtComentario_string_df = spark.read.schema(subreclamos_txtComentario_string_json.schema).json(ar_path)
    subreclamos_txtComentario_string_df = subreclamos_txtComentario_string_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","txtComentario.value as txtComentario")
    subreclamos_txtComentario_string_df = subreclamos_txtComentario_string_df.filter(subreclamos_txtComentario_string_df.txtComentario.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(subreclamos_txtComentario_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_txtComentario_string")
    
    """txtComentario como String"""
    subreclamos_txtComentario_array_payload = ["{'UniversalID':'string','txtComentario':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}}"]
    sc = spark.sparkContext
    subreclamos_txtComentario_array_rdd = sc.parallelize(subreclamos_txtComentario_array_payload)
    subreclamos_txtComentario_array_json = spark.read.json(subreclamos_txtComentario_array_rdd)
    subreclamos_txtComentario_array_df = spark.read.schema(subreclamos_txtComentario_array_json.schema).json(ar_path)
    subreclamos_txtComentario_array_df = subreclamos_txtComentario_array_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","txtComentario.value as txtComentario")
    subreclamos_txtComentario_array_df = subreclamos_txtComentario_array_df.filter(col("UniversalID").isNotNull())
    subreclamos_txtComentario_array_df = subreclamos_txtComentario_array_df.select("UniversalID","dtFechaCreacion",array_join("txtComentario", '\r\n ').alias("txtComentario"))
    sqlContext.registerDataFrameAsTable(subreclamos_txtComentario_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_txtComentario_array")
    
    subreclamo_txtComentario_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_txtComentario_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_txtComentario_array")
    sqlContext.registerDataFrameAsTable(subreclamo_txtComentario_df, "Zdc_Lotus_subreclamos_Tablaintermedia_txtComentario")
    
    """txtHistoria como String"""
    subreclamos_txtHistoria_string_payload =  ["{'UniversalID':'string','txtHistoria':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    subreclamos_txtHistoria_string_rdd = sc.parallelize(subreclamos_txtHistoria_string_payload)
    subreclamos_txtHistoria_string_json = spark.read.json(subreclamos_txtHistoria_string_rdd)
    subreclamos_txtHistoria_string_df = spark.read.schema(subreclamos_txtHistoria_string_json.schema).json(ar_path)
    subreclamos_txtHistoria_string_df = subreclamos_txtHistoria_string_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","txtHistoria.value as txtHistoria")
    subreclamos_txtHistoria_string_df = subreclamos_txtHistoria_string_df.filter(subreclamos_txtHistoria_string_df.txtHistoria.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(subreclamos_txtHistoria_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_txtHistoria_string")
    
    """txtHistoria como String"""
    subreclamos_txtHistoria_array_payload = ["{'UniversalID':'string','txtHistoria':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}}"]
    sc = spark.sparkContext
    subreclamos_txtHistoria_array_rdd = sc.parallelize(subreclamos_txtHistoria_array_payload)
    subreclamos_txtHistoria_array_json = spark.read.json(subreclamos_txtHistoria_array_rdd)
    subreclamos_txtHistoria_array_df = spark.read.schema(subreclamos_txtHistoria_array_json.schema).json(ar_path)
    subreclamos_txtHistoria_array_df = subreclamos_txtHistoria_array_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","txtHistoria.value as txtHistoria")
    subreclamos_txtHistoria_array_df = subreclamos_txtHistoria_array_df.filter(col("UniversalID").isNotNull())
    subreclamos_txtHistoria_array_df = subreclamos_txtHistoria_array_df.select("UniversalID","dtFechaCreacion",array_join("txtHistoria", '\r\n ').alias("txtHistoria"))
    sqlContext.registerDataFrameAsTable(subreclamos_txtHistoria_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_txtHistoria_array")
    
    subreclamo_txtHistoria_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_txtHistoria_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_txtHistoria_array")
    sqlContext.registerDataFrameAsTable(subreclamo_txtHistoria_df, "Zdc_Lotus_subreclamos_Tablaintermedia_txtHistoria")
    
    ValorY = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_LOTUS_RECLAMOS_APP' LIMIT 1").first()["Valor1"]
    
    panel_principal_subreclamos = sqlContext.sql("\
    SELECT \
      UniversalID, \
      txtIdDocPadre.value UniversalID_Sub, \
      txtRadicado.value Subreclamo, \
      dtFechaCreacion.value FechaCreacion, \
      cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4)as int) anio, \
      cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes, \
      txtAutor.value Autor, \
      txtTipoProducto.value Producto, \
      txtTipoReclamo.value TipoReclamo, \
      IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/subreclamos/', UniversalID,'.zip'" + "), '') link \
    FROM \
      Zdc_Lotus_SubReclamos_TablaIntermedia")
    
    panel_detalle_subreclamos = sqlContext.sql("\
    SELECT UniversalID, 'Datos Generales' AS vista, 1 as posicion, 'Autor' AS key, txtAutor.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Datos Generales' AS vista, 2 as posicion, 'FechaCreacion' AS key, dtFechaCreacion.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Datos Generales' AS vista, 3 as posicion, 'Responsable' AS key, cbResponsable.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Datos Generales' AS vista, 4 as posicion, 'Estado' AS key, txtEstado.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Datos Generales' AS vista, 5 as posicion, 'Subreclamo' AS key, txtRadicado.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 6 as posicion, 'NumeroIdentificacion' AS key, txtNumIdCliente.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 7 as posicion, 'Nombre' AS key, txtNomCliente.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 8 as posicion, 'Producto' AS key, txtTipoProducto.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 9 as posicion, 'TipoReclamo' AS key, txtTipoReclamo.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 10 as posicion, 'NumeroProducto' AS key, concat(txtPrefijo.value,'-',txtNumProducto.value) AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 11 as posicion, 'TipoCuenta' AS key, rdTipoCuenta.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 12 as posicion, 'NumeroCuenta' AS key, txtNumCuenta.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 13 as posicion, 'ValorTotalPesos' AS key, numValorPesos.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 14 as posicion, 'ValorGMF' AS key, numValorGMF.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 15 as posicion, 'ValorComisiones' AS key, numValorComisiones.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 16 as posicion, 'ValorReversion' AS key, numValorReversion.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 17 as posicion, 'ClienteTieneRazon' AS key, rdClienteRazon.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 18 as posicion, 'CampoDiligenciar' AS key, cbCampoDiligencia.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 19 as posicion, 'CargaCliente' AS key, rdCargaCliente.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 20 as posicion, 'SucursalSobrante' AS key, cbSucursalSobrante.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 21 as posicion, 'FechaSobrante' AS key, dtFechaSobrante.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 22 as posicion, 'PyGSucursalError' AS key, cbSucursalError.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 23 as posicion, 'PyGAreaAdministrativa' AS key, txtAreaAdministrativa.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 24 as posicion, 'CuentaTercero' AS key, txtCuentaTercero.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 25 as posicion, 'TipoCuentaTercero' AS key, rdTipoCuentaTercero.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Subreclamos' AS vista, 26 as posicion, 'PyGTarjetas' AS key, cbTarjetas AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_subreclamos_Tablaintermedia_cbTarjetas \
    UNION ALL \
    SELECT UniversalID, 'Fecha de Solucion' AS vista, 27 as posicion, 'TiempoRealSolucion' AS key, numTiempoSolucionReal.value  AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Fecha de Solucion' AS vista, 28 as posicion, 'FechaRealSolucion' AS key, dtFechaSolucionReal.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalID, 'Comentario' AS vista, 29 as posicion, 'Comentario' AS key, txtComentario AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_subreclamos_Tablaintermedia_txtComentario \
    UNION ALL \
    SELECT UniversalID, 'Historia' AS vista, 30 as posicion, 'Historia' AS key, txtHistoria AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_subreclamos_Tablaintermedia_txtHistoria")

      
    return subreclamos_df, panel_principal_subreclamos, panel_detalle_subreclamos
  except Exception as Error:
    print(Error, "\nError validando auditoria subreclamos diseño nuevo")

# COMMAND ----------

def subreclamos_old(ar_path):
  try:
    subreclamos_old_df = spark.read.schema(ft_schema_subreclamos_old().schema).json(ar_path)
    sqlContext.registerDataFrameAsTable(subreclamos_old_df, "Zdc_Lotus_SubReclamos_old_TablaIntermedia")
    
    """key_PGTarjetas como String"""
    subreclamos_old_key_PGTarjetas_string_payload =  ["{'UniversalID':'string','key_PGTarjetas':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    subreclamos_old_key_PGTarjetas_string_rdd = sc.parallelize(subreclamos_old_key_PGTarjetas_string_payload)
    subreclamos_old_key_PGTarjetas_string_json = spark.read.json(subreclamos_old_key_PGTarjetas_string_rdd)
    subreclamos_old_key_PGTarjetas_string_df = spark.read.schema(subreclamos_old_key_PGTarjetas_string_json.schema).json(ar_path)
    subreclamos_old_key_PGTarjetas_string_df = subreclamos_old_key_PGTarjetas_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","key_PGTarjetas.value as key_PGTarjetas")
    subreclamos_old_key_PGTarjetas_string_df = subreclamos_old_key_PGTarjetas_string_df.filter(subreclamos_old_key_PGTarjetas_string_df.key_PGTarjetas.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(subreclamos_old_key_PGTarjetas_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_key_PGTarjetas_string")
    
    """key_PGTarjetas como String"""
    subreclamos_old_key_PGTarjetas_array_payload = ["{'UniversalID':'string','key_PGTarjetas':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}}"]
    sc = spark.sparkContext
    subreclamos_old_key_PGTarjetas_array_rdd = sc.parallelize(subreclamos_old_key_PGTarjetas_array_payload)
    subreclamos_old_key_PGTarjetas_array_json = spark.read.json(subreclamos_old_key_PGTarjetas_array_rdd)
    subreclamos_old_key_PGTarjetas_array_df = spark.read.schema(subreclamos_old_key_PGTarjetas_array_json.schema).json(ar_path)
    subreclamos_old_key_PGTarjetas_array_df = subreclamos_old_key_PGTarjetas_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","key_PGTarjetas.value as key_PGTarjetas")
    subreclamos_old_key_PGTarjetas_array_df = subreclamos_old_key_PGTarjetas_array_df.filter(col("UniversalID").isNotNull())
    subreclamos_old_key_PGTarjetas_array_df = subreclamos_old_key_PGTarjetas_array_df.select("UniversalID","dat_FchCreac",array_join("key_PGTarjetas", '\r\n ').alias("key_PGTarjetas"))
    sqlContext.registerDataFrameAsTable(subreclamos_old_key_PGTarjetas_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_key_PGTarjetas_array")
    
    subreclamos_old_cbTarjetas_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_key_PGTarjetas_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_key_PGTarjetas_array")
    sqlContext.registerDataFrameAsTable(subreclamos_old_cbTarjetas_df, "Zdc_Lotus_subreclamos_Tablaintermedia_key_PGTarjetas")
    
    """txt_Comentarios como String"""
    subreclamos_old_txt_Comentarios_string_payload =  ["{'UniversalID':'string','txt_Comentarios':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]

    sc = spark.sparkContext
    subreclamos_old_txt_Comentarios_string_rdd = sc.parallelize(subreclamos_old_txt_Comentarios_string_payload)
    subreclamos_old_txt_Comentarios_string_json = spark.read.json(subreclamos_old_txt_Comentarios_string_rdd)
    subreclamos_old_txt_Comentarios_string_df = spark.read.schema(subreclamos_old_txt_Comentarios_string_json.schema).json(ar_path)
    subreclamos_old_txt_Comentarios_string_df = subreclamos_old_txt_Comentarios_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Comentarios.value as txt_Comentarios")
    subreclamos_old_txt_Comentarios_string_df = subreclamos_old_txt_Comentarios_string_df.filter(subreclamos_old_txt_Comentarios_string_df.txt_Comentarios.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(subreclamos_old_txt_Comentarios_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_txt_Comentarios_string")
    
    """txt_Comentarios como String"""
    subreclamos_old_txt_Comentarios_array_payload = ["{'UniversalID':'string','txt_Comentarios':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}}"]
    sc = spark.sparkContext
    subreclamos_old_txt_Comentarios_array_rdd = sc.parallelize(subreclamos_old_txt_Comentarios_array_payload)
    subreclamos_old_txt_Comentarios_array_json = spark.read.json(subreclamos_old_txt_Comentarios_array_rdd)
    subreclamos_old_txt_Comentarios_array_df = spark.read.schema(subreclamos_old_txt_Comentarios_array_json.schema).json(ar_path)
    subreclamos_old_txt_Comentarios_array_df = subreclamos_old_txt_Comentarios_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Comentarios.value as txt_Comentarios")
    subreclamos_old_txt_Comentarios_array_df = subreclamos_old_txt_Comentarios_array_df.filter(col("UniversalID").isNotNull())
    subreclamos_old_txt_Comentarios_array_df = subreclamos_old_txt_Comentarios_array_df.select("UniversalID","dat_FchCreac",array_join("txt_Comentarios", '\r\n ').alias("txt_Comentarios"))
    sqlContext.registerDataFrameAsTable(subreclamos_old_txt_Comentarios_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_txt_Comentarios_array")
    
    subreclamos_old_txt_Comentarios_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_txt_Comentarios_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_txt_Comentarios_array")
    sqlContext.registerDataFrameAsTable(subreclamos_old_txt_Comentarios_df, "Zdc_Lotus_subreclamos_Tablaintermedia_txt_Comentarios")
    
    """txt_Historia como String"""
    subreclamos_old_txt_Historia_string_payload =  ["{'UniversalID':'string','txt_Historia':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    subreclamos_old_txt_Historia_string_rdd = sc.parallelize(subreclamos_old_txt_Historia_string_payload)
    subreclamos_old_txt_Historia_string_json = spark.read.json(subreclamos_old_txt_Historia_string_rdd)
    subreclamos_old_txt_Historia_string_df = spark.read.schema(subreclamos_old_txt_Historia_string_json.schema).json(ar_path)
    subreclamos_old_txt_Historia_string_df = subreclamos_old_txt_Historia_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
    subreclamos_old_txt_Historia_string_df = subreclamos_old_txt_Historia_string_df.filter(subreclamos_old_txt_Historia_string_df.txt_Historia.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(subreclamos_old_txt_Historia_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_txt_Historia_string")
    
    """txt_Historia como String"""
    subreclamos_old_txt_Historia_array_payload = ["{'UniversalID':'string','txt_Historia':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}}"]
    sc = spark.sparkContext
    subreclamos_old_txt_Historia_array_rdd = sc.parallelize(subreclamos_old_txt_Historia_array_payload)
    subreclamos_old_txt_Historia_array_json = spark.read.json(subreclamos_old_txt_Historia_array_rdd)
    subreclamos_old_txt_Historia_array_df = spark.read.schema(subreclamos_old_txt_Historia_array_json.schema).json(ar_path)
    subreclamos_old_txt_Historia_array_df = subreclamos_old_txt_Historia_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
    subreclamos_old_txt_Historia_array_df = subreclamos_old_txt_Historia_array_df.filter(col("UniversalID").isNotNull())
    subreclamos_old_txt_Historia_array_df = subreclamos_old_txt_Historia_array_df.select("UniversalID","dat_FchCreac",array_join("txt_Historia", '\r\n ').alias("txt_Historia"))
    sqlContext.registerDataFrameAsTable(subreclamos_old_txt_Historia_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_txt_Historia_array")
    
    subreclamos_old_txt_Historia_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_txt_Historia_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_txt_Historia_array")
    sqlContext.registerDataFrameAsTable(subreclamos_old_txt_Historia_df, "Zdc_Lotus_subreclamos_Tablaintermedia_txt_Historia")
    
    ValorY = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_LOTUS_RECLAMOS_APP' LIMIT 1").first()["Valor1"]
    
    sql_string = "SELECT \
      UniversalId, \
      dat_FchCreac.value FechaCreacion, \
      substring_index(dat_FchCreac.value, ' ', 1)fecha_solicitudes, \
      cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4)as int) anio, \
      cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2)as int) mes, \
      key_Estado.value Estado, \
      txt_NomCliente.value NomCliente, \
      key_Producto.value Producto, \
      key_TipoReclamo.value TipoReclamo, \
      SSREF.value UniversalID_Padre, \
      IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/subreclamos-old/', UniversalID,'.zip'" + "), '') link \
    FROM \
      Zdc_Lotus_SubReclamos_old_TablaIntermedia"

    panel_principal_subreclamos_old = sqlContext.sql(sql_string)

    panel_detalle_subreclamos_old = sqlContext.sql("\
    SELECT UniversalId, 'Servicio al Cliente' AS vista, 1 as posicion, 'Días vencidos Totales' AS key, num_DV.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Servicio al Cliente' AS vista, 2 as posicion, 'Días vencidos Abono Temporal' AS key, num_DVSlnado.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, '' AS vista, 3 as posicion, 'Autor' AS key, txt_Autor.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, '' AS vista, 4 as posicion, 'Creador' AS key, txt_Creador.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, '' AS vista, 5 as posicion, 'Fecha Creación' AS key, dat_FchCreac.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, '' AS vista, 6 as posicion, 'Número de Radicación del Subreclamo' AS key, txt_Radicado.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, '' AS vista, 7 as posicion, ' ' AS key, key_Estado.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, '' AS vista, 8 as posicion, 'Estado Subreclamo' AS key, key_Estado_1.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Asignación' AS vista, 9 as posicion, 'Responsable' AS key, nom_Responsable.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del subreclamo' AS vista, 10 as posicion, 'Cédula/Nit' AS key, txt_CedNit.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del subreclamo' AS vista, 11 as posicion, 'Nombre' AS key, txt_NomCliente.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del subreclamo' AS vista, 12 as posicion, 'Producto' AS key, key_Producto.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del subreclamo' AS vista, 13 as posicion, 'Tipo de Reclamo' AS key, key_TipoReclamo.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del subreclamo' AS vista, 14 as posicion, 'Número de Producto' AS key, concat(cast((key_Prefijo.value) as string), ' - ',  cast((txt_NumProducto.value)as string)) AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del subreclamo' AS vista, 15 as posicion, 'Tipo de Cuenta' AS key, CASE \
        WHEN UPPER(key_TipoCuenta.value) = 'S' THEN \
          'Ahorros' \
        WHEN UPPER(key_TipoCuenta.value) = 'D' THEN \
          'Corriente' \
        ELSE \
          '' \
      END  AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del subreclamo' AS vista, 16 as posicion, 'Número de Cuenta' AS key, num_NroCuenta.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del subreclamo' AS vista, 18 as posicion, 'Valor GMF' AS key, num_VlrGMF.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del subreclamo' AS vista, 19 as posicion, 'Valor Comisiones' AS key, num_VlrComis.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del subreclamo' AS vista, 20 as posicion, 'Valor Reversión' AS key, num_VlrRever.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del subreclamo' AS vista, 21 as posicion, 'El Cliente Tiene la Razón?' AS key, key_ClienteRazon.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del subreclamo' AS vista, 22 as posicion, 'Se le Carga al Cliente?' AS key, key_CargaClte.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del subreclamo' AS vista, 23 as posicion, 'Campo a Diligenciar' AS key, CASE \
        WHEN UPPER(key_CampoDilig.value) = 'C1' THEN \
          'P y G Sucursal del Error' \
        WHEN UPPER(key_CampoDilig.value) = 'C2' THEN \
          'Cuenta de Tercero' \
        WHEN UPPER(key_CampoDilig.value) = 'N' THEN \
          'Ninguno' \
        ELSE \
          '' \
      END AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del subreclamo' AS vista, 24 as posicion, 'P y G Sucursal Error' AS key, key_PGSucError.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del subreclamo' AS vista, 25 as posicion, 'P y G Area Administrativa' AS key, key_PGAreaAdmin.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del subreclamo' AS vista, 26 as posicion, 'Cuenta Tercero' AS key, num_NroCtaTercero.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del subreclamo' AS vista, 27 as posicion, 'Tipo de Cuenta del Tercero' AS key, CASE \
        WHEN UPPER(key_TipoCtaTercero.value) = 'S' THEN \
          'Ahorros' \
        WHEN UPPER(key_TipoCtaTercero.value) = 'D' THEN \
          'Corriente' \
        ELSE \
          '' \
      END AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del subreclamo' AS vista, 28 as posicion, 'P y G tarjetas' AS key, key_PGTarjetas AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_subreclamos_Tablaintermedia_key_PGTarjetas \
    UNION ALL \
    SELECT UniversalId, 'Datos de Fechas de Solución' AS vista, 29 as posicion, 'Tiempo Real de la Solución' AS key, num_DAbierto.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Fechas de Solución' AS vista, 30 as posicion, 'Fecha Real de la Solución' AS key, dat_Solucionado.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Comentarios' AS vista, 31 as posicion, '' AS key, txt_Comentarios AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_subreclamos_Tablaintermedia_txt_Comentarios \
    UNION ALL \
    SELECT UniversalId, 'Historia' AS vista, 32 as posicion, '' AS key, txt_Historia AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_subreclamos_Tablaintermedia_txt_Historia")
    
    return subreclamos_old_df, panel_principal_subreclamos_old, panel_detalle_subreclamos_old
  except Exception as Error:
    print(Error, "\nError validando auditoria subreclamos diseño viejo")

# COMMAND ----------

def ft_reclamos(ar_path):    
  try:
    reclamos_df = spark.read.schema(ft_schema_reclamos().schema).json(ar_path)
    sqlContext.registerDataFrameAsTable(reclamos_df, "Zdc_Lotus_Reclamos_TablaIntermedia_Aux")
    
    
    reclamos_df = sqlContext.sql("\
      SELECT \
          if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, \
          UniversalIDCompuesto, \
          UniversalID as UniversalIDPadre, \
          TituloDB, \
          NumTotVencidos, \
          TXTSUCCAPTURA, \
          attachments, \
          cbCiudadCliente, \
          cbCiudadTransaccion, \
          cbOrigenReporte, \
          cbProductoAfectado, \
          cbReportadoPor, \
          cbResponsableError, \
          cbSucursalRadicacion, \
          cbSucursalVarios, \
          cbTipoPoliza, \
          dtFechaCreacion, \
          dtFechaSlnRpta, \
          dtFechaSolucionReal, \
          dtFechaTransaccion, \
          numAbonoDefinitivo, \
          numAbonoTemporal, \
          numTiempoSlnRpta, \
          numTiempoSolucion, \
          numTiempoSolucionReal, \
          numTotContabiliza, \
          numTotDias, \
          numValorDolares, \
          numValorPesos, \
          rdAutorizaEmail, \
          rdClienteRazon, \
          rdTodosSoportes, \
          rtPantallasReclamo, \
          rtPantallasSolucion, \
          txtAutor, \
          txtCelular, \
          txtClasificacion, \
          txtCodCanal, \
          txtCodCiudadCliente, \
          txtCodConvenio, \
          txtCodigoCompra, \
          txtCodigoEstablecimiento, \
          txtCodigoProducto, \
          txtDeptoCliente, \
          txtDireccionCliente, \
          txtEmailCliente, \
          txtFax, \
          txtFuncionarioEmpresa, \
          txtGerenteCuenta, \
          txtNitEmpresa, \
          txtNomCanal, \
          txtNomCliente, \
          txtNomEmpresa, \
          txtNomRespError, \
          txtNumEncargoFiducia, \
          txtNumIdCliente, \
          txtNumPoliza, \
          txtNumProducto, \
          txtNumTarjetaCtaDebita, \
          txtNumTarjetaDebito, \
          txtRadicado, \
          txtResponsable, \
          txtSegmento, \
          txtSoportesNoRecibidos, \
          txtSoportesRecibidos, \
          txtTelefonosCliente, \
          txtTipoProducto, \
          txtTipoReclamo \
      FROM \
          Zdc_Lotus_Reclamos_TablaIntermedia_Aux")
    sqlContext.registerDataFrameAsTable(reclamos_df, "Zdc_Lotus_Reclamos_TablaIntermedia")

    """txtComentario como String"""
    reclamos_txtComentario_string_payload =  ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtComentario':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_txtComentario_string_rdd = sc.parallelize(reclamos_txtComentario_string_payload)
    reclamos_txtComentario_string_json = spark.read.json(reclamos_txtComentario_string_rdd)
    reclamos_txtComentario_string_df = spark.read.schema(reclamos_txtComentario_string_json.schema).json(ar_path)
    reclamos_txtComentario_string_df = reclamos_txtComentario_string_df.selectExpr("UniversalID","UniversalIDCompuesto","dtFechaCreacion.value as dtFechaCreacion","txtComentario.value as txtComentario")
    reclamos_txtComentario_string_df = reclamos_txtComentario_string_df.filter(reclamos_txtComentario_string_df.txtComentario.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(reclamos_txtComentario_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtComentario_string")
   
    reclamos_txtComentario_array_payload = ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtComentario':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}}"]
    sc = spark.sparkContext
    reclamos_txtComentario_array_rdd = sc.parallelize(reclamos_txtComentario_array_payload)
    reclamos_txtComentario_array_json = spark.read.json(reclamos_txtComentario_array_rdd)
    reclamos_txtComentario_array_df = spark.read.schema(reclamos_txtComentario_array_json.schema).json(ar_path)
    reclamos_txtComentario_array_df = reclamos_txtComentario_array_df.selectExpr("UniversalID","UniversalIDCompuesto","dtFechaCreacion.value as dtFechaCreacion","txtComentario.value as txtComentario")
    reclamos_txtComentario_array_df = reclamos_txtComentario_array_df.filter(col("UniversalID").isNotNull())
    reclamos_txtComentario_array_df = reclamos_txtComentario_array_df.select("UniversalID","UniversalIDCompuesto","dtFechaCreacion",array_join("txtComentario", '\r\n ').alias("txtComentario"))
    sqlContext.registerDataFrameAsTable(reclamos_txtComentario_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtComentario_array")

    reclamos_txtComentario_df = sqlContext.sql("SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion,txtComentario \
                                                FROM \
                                                   Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtComentario_string \
                                                UNION \
                                                SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion,txtComentario \
                                                FROM \
                                                   Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtComentario_array")

    sqlContext.registerDataFrameAsTable(reclamos_txtComentario_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txtComentario")
    
    """txtMensajeAyuda como String"""
    reclamos_txtMensajeAyuda_string_payload =  ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtMensajeAyuda':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]

    sc = spark.sparkContext
    reclamos_txtMensajeAyuda_string_rdd = sc.parallelize(reclamos_txtMensajeAyuda_string_payload)
    reclamos_txtMensajeAyuda_string_json = spark.read.json(reclamos_txtMensajeAyuda_string_rdd)
    reclamos_txtMensajeAyuda_string_df = spark.read.schema(reclamos_txtMensajeAyuda_string_json.schema).json(ar_path)
    reclamos_txtMensajeAyuda_string_df = reclamos_txtMensajeAyuda_string_df.selectExpr("UniversalID","UniversalIDCompuesto","dtFechaCreacion.value as dtFechaCreacion","txtMensajeAyuda.value as txtMensajeAyuda")
    reclamos_txtMensajeAyuda_string_df = reclamos_txtMensajeAyuda_string_df.filter(reclamos_txtMensajeAyuda_string_df.txtMensajeAyuda.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(reclamos_txtMensajeAyuda_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtMensajeAyuda_string")

    reclamos_txtMensajeAyuda_array_payload = ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtMensajeAyuda':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}"]

    sc = spark.sparkContext
    reclamos_txtMensajeAyuda_array_rdd = sc.parallelize(reclamos_txtMensajeAyuda_array_payload)
    reclamos_txtMensajeAyuda_array_json = spark.read.json(reclamos_txtMensajeAyuda_array_rdd)
    reclamos_txtMensajeAyuda_array_df = spark.read.schema(reclamos_txtMensajeAyuda_array_json.schema).json(ar_path)
    reclamos_txtMensajeAyuda_array_df = reclamos_txtMensajeAyuda_array_df.selectExpr("UniversalID","UniversalIDCompuesto","dtFechaCreacion.value as dtFechaCreacion","txtMensajeAyuda.value as txtMensajeAyuda")
    reclamos_txtMensajeAyuda_array_df = reclamos_txtMensajeAyuda_array_df.filter(col("UniversalID").isNotNull())
    reclamos_txtMensajeAyuda_array_df = reclamos_txtMensajeAyuda_array_df.select("UniversalID","UniversalIDCompuesto","dtFechaCreacion",array_join("txtMensajeAyuda", '\r\n ').alias("txtMensajeAyuda"))
    
    sqlContext.registerDataFrameAsTable(reclamos_txtMensajeAyuda_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtMensajeAyuda_array")
    
    reclamos_txtMensajeAyuda_df = sqlContext.sql("SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion,txtMensajeAyuda \
                                                  FROM \
                                                     Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtMensajeAyuda_string \
                                                  UNION \
                                                  SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID,dtFechaCreacion, txtMensajeAyuda \
                                                  FROM \
                                                     Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtMensajeAyuda_array")
    sqlContext.registerDataFrameAsTable(reclamos_txtMensajeAyuda_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txtMensajeAyuda")

    """txtObservaciones como String"""
    reclamos_txtObservaciones_string_payload =  ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtObservaciones':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_txtObservaciones_string_rdd = sc.parallelize(reclamos_txtObservaciones_string_payload)
    reclamos_txtObservaciones_string_json = spark.read.json(reclamos_txtObservaciones_string_rdd)
    reclamos_txtObservaciones_string_df = spark.read.schema(reclamos_txtObservaciones_string_json.schema).json(ar_path)
    reclamos_txtObservaciones_string_df = reclamos_txtObservaciones_string_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtObservaciones.value as txtObservaciones")
    reclamos_txtObservaciones_string_df = reclamos_txtObservaciones_string_df.filter(reclamos_txtObservaciones_string_df.txtObservaciones.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(reclamos_txtObservaciones_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtObservaciones_string")
    reclamos_txtObservaciones_array_payload = ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtObservaciones':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}"]

    sc = spark.sparkContext
    reclamos_txtObservaciones_array_rdd = sc.parallelize(reclamos_txtObservaciones_array_payload)
    reclamos_txtObservaciones_array_json = spark.read.json(reclamos_txtObservaciones_array_rdd)
    reclamos_txtObservaciones_array_df = spark.read.schema(reclamos_txtObservaciones_array_json.schema).json(ar_path)
    reclamos_txtObservaciones_array_df = reclamos_txtObservaciones_array_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtObservaciones.value as txtObservaciones")
    reclamos_txtObservaciones_array_df = reclamos_txtObservaciones_array_df.filter(col("UniversalID").isNotNull())
    reclamos_txtObservaciones_array_df = reclamos_txtObservaciones_array_df.select("UniversalID","UniversalIDCompuesto", "dtFechaCreacion",array_join("txtObservaciones", '\r\n ').alias("txtObservaciones"))
    sqlContext.registerDataFrameAsTable(reclamos_txtObservaciones_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtObservaciones_array")
    
    reclamos_txtObservaciones_df = sqlContext.sql("SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, txtObservaciones \
                                                   FROM \
                                                      Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtObservaciones_string \
                                                   UNION \
                                                   SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, txtObservaciones \
                                                   FROM  \
                                                      Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtObservaciones_array")
    sqlContext.registerDataFrameAsTable(reclamos_txtObservaciones_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txtObservaciones")
    
    """txtSoportesRequeridos como String"""
    reclamos_txtSoportesRequeridos_string_payload =  ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtSoportesRequeridos':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]

    sc = spark.sparkContext
    reclamos_txtSoportesRequeridos_string_rdd = sc.parallelize(reclamos_txtSoportesRequeridos_string_payload)
    reclamos_txtSoportesRequeridos_string_json = spark.read.json(reclamos_txtSoportesRequeridos_string_rdd)
    reclamos_txtSoportesRequeridos_string_df = spark.read.schema(reclamos_txtSoportesRequeridos_string_json.schema).json(ar_path)
    reclamos_txtSoportesRequeridos_string_df = reclamos_txtSoportesRequeridos_string_df.selectExpr("UniversalID", "UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtSoportesRequeridos.value as txtSoportesRequeridos")
    reclamos_txtSoportesRequeridos_string_df = reclamos_txtSoportesRequeridos_string_df.filter(reclamos_txtSoportesRequeridos_string_df.txtSoportesRequeridos.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(reclamos_txtSoportesRequeridos_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtSoportesRequeridos_string")
    
    reclamos_txtSoportesRequeridos_array_payload = ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtSoportesRequeridos':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_txtSoportesRequeridos_array_rdd = sc.parallelize(reclamos_txtSoportesRequeridos_array_payload)
    reclamos_txtSoportesRequeridos_array_json = spark.read.json(reclamos_txtSoportesRequeridos_array_rdd)
    reclamos_txtSoportesRequeridos_array_df = spark.read.schema(reclamos_txtSoportesRequeridos_array_json.schema).json(ar_path)
    reclamos_txtSoportesRequeridos_array_df = reclamos_txtSoportesRequeridos_array_df.selectExpr("UniversalID","UniversalIDCompuesto","dtFechaCreacion.value as dtFechaCreacion","txtSoportesRequeridos.value as txtSoportesRequeridos")
    reclamos_txtSoportesRequeridos_array_df = reclamos_txtSoportesRequeridos_array_df.filter(col("UniversalID").isNotNull())
    reclamos_txtSoportesRequeridos_array_df = reclamos_txtSoportesRequeridos_array_df.select("UniversalID","UniversalIDCompuesto","dtFechaCreacion",array_join("txtSoportesRequeridos", '\r\n ').alias("txtSoportesRequeridos"))
    sqlContext.registerDataFrameAsTable(reclamos_txtSoportesRequeridos_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtSoportesRequeridos_array")
    
    reclamos_txtSoportesRequeridos_df = sqlContext.sql("SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, txtSoportesRequeridos \
                                                        FROM \
                                                           Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtSoportesRequeridos_string \
                                                        UNION \
                                                        SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, txtSoportesRequeridos \
                                                        FROM \
                                                           Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtSoportesRequeridos_array")
    sqlContext.registerDataFrameAsTable(reclamos_txtSoportesRequeridos_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txtSoportesRequeridos")
    
    """txtPantallasReclamo como String"""
    reclamos_txtPantallasReclamo_string_payload =  ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtPantallasReclamo':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]

    sc = spark.sparkContext
    reclamos_txtPantallasReclamo_string_rdd = sc.parallelize(reclamos_txtPantallasReclamo_string_payload)
    reclamos_txtPantallasReclamo_string_json = spark.read.json(reclamos_txtPantallasReclamo_string_rdd)
    reclamos_txtPantallasReclamo_string_df = spark.read.schema(reclamos_txtPantallasReclamo_string_json.schema).json(ar_path)
    reclamos_txtPantallasReclamo_string_df = reclamos_txtPantallasReclamo_string_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtPantallasReclamo.value as txtPantallasReclamo")
    reclamos_txtPantallasReclamo_string_df = reclamos_txtPantallasReclamo_string_df.filter(reclamos_txtPantallasReclamo_string_df.txtPantallasReclamo.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(reclamos_txtPantallasReclamo_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtPantallasReclamo_string")
    
    reclamos_txtPantallasReclamo_array_payload = ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtPantallasReclamo':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}"]

    sc = spark.sparkContext
    reclamos_txtPantallasReclamo_array_rdd = sc.parallelize(reclamos_txtPantallasReclamo_array_payload)
    reclamos_txtPantallasReclamo_array_json = spark.read.json(reclamos_txtPantallasReclamo_array_rdd)
    reclamos_txtPantallasReclamo_array_df = spark.read.schema(reclamos_txtPantallasReclamo_array_json.schema).json(ar_path)
    reclamos_txtPantallasReclamo_array_df = reclamos_txtPantallasReclamo_array_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtPantallasReclamo.value as txtPantallasReclamo")
    reclamos_txtPantallasReclamo_array_df = reclamos_txtPantallasReclamo_array_df.filter(col("UniversalID").isNotNull())
    reclamos_txtPantallasReclamo_array_df = reclamos_txtPantallasReclamo_array_df.select("UniversalID","UniversalIDCompuesto", "dtFechaCreacion", array_join("txtPantallasReclamo", '\r\n ').alias("txtPantallasReclamo"))
    sqlContext.registerDataFrameAsTable(reclamos_txtPantallasReclamo_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtPantallasReclamo_array")
    
    reclamos_txtPantallasReclamo_df = sqlContext.sql("SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, txtPantallasReclamo \
                                                      FROM \
                                                         Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtPantallasReclamo_string \
                                                      UNION \
                                                      SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, txtPantallasReclamo \
                                                      FROM \
                                                         Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtPantallasReclamo_array")
    sqlContext.registerDataFrameAsTable(reclamos_txtPantallasReclamo_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txtPantallasReclamo")
    
    """txtPantallasSolucion como String"""
    reclamos_txtPantallasSolucion_string_payload =  ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtPantallasSolucion':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_txtPantallasSolucion_string_rdd = sc.parallelize(reclamos_txtPantallasSolucion_string_payload)
    reclamos_txtPantallasSolucion_string_json = spark.read.json(reclamos_txtPantallasSolucion_string_rdd)
    reclamos_txtPantallasSolucion_string_df = spark.read.schema(reclamos_txtPantallasSolucion_string_json.schema).json(ar_path)
    reclamos_txtPantallasSolucion_string_df = reclamos_txtPantallasSolucion_string_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtPantallasSolucion.value as txtPantallasSolucion")
    reclamos_txtPantallasSolucion_string_df = reclamos_txtPantallasSolucion_string_df.filter(reclamos_txtPantallasSolucion_string_df.txtPantallasSolucion.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(reclamos_txtPantallasSolucion_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtPantallasSolucion_string")
    
    reclamos_txtPantallasSolucion_array_payload = ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtPantallasSolucion':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}"]

    sc = spark.sparkContext
    reclamos_txtPantallasSolucion_array_rdd = sc.parallelize(reclamos_txtPantallasSolucion_array_payload)
    reclamos_txtPantallasSolucion_array_json = spark.read.json(reclamos_txtPantallasSolucion_array_rdd)
    reclamos_txtPantallasSolucion_array_df = spark.read.schema(reclamos_txtPantallasSolucion_array_json.schema).json(ar_path)
    reclamos_txtPantallasSolucion_array_df = reclamos_txtPantallasSolucion_array_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtPantallasSolucion.value as txtPantallasSolucion")
    reclamos_txtPantallasSolucion_array_df = reclamos_txtPantallasSolucion_array_df.filter(col("UniversalID").isNotNull())
    reclamos_txtPantallasSolucion_array_df = reclamos_txtPantallasSolucion_array_df.select("UniversalID","UniversalIDCompuesto", "dtFechaCreacion", array_join("txtPantallasSolucion", '\r\n ').alias("txtPantallasSolucion"))
    sqlContext.registerDataFrameAsTable(reclamos_txtPantallasSolucion_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtPantallasSolucion_array")
    
    reclamos_txtPantallasSolucion_df = sqlContext.sql("SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, txtPantallasSolucion \
                                                       FROM \
                                                          Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtPantallasSolucion_string \
                                                       UNION \
                                                       SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, txtPantallasSolucion \
                                                       FROM \
                                                          Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtPantallasSolucion_array")
    sqlContext.registerDataFrameAsTable(reclamos_txtPantallasSolucion_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txtPantallasSolucion")

    """txtRespuesta como String"""
    reclamos_txtRespuesta_string_payload =  ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtRespuesta':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_txtRespuesta_string_rdd = sc.parallelize(reclamos_txtRespuesta_string_payload)
    reclamos_txtRespuesta_string_json = spark.read.json(reclamos_txtRespuesta_string_rdd)
    reclamos_txtRespuesta_string_df = spark.read.schema(reclamos_txtRespuesta_string_json.schema).json(ar_path)
    reclamos_txtRespuesta_string_df = reclamos_txtRespuesta_string_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtRespuesta.value as txtRespuesta")
    reclamos_txtRespuesta_string_df = reclamos_txtRespuesta_string_df.filter(reclamos_txtRespuesta_string_df.txtRespuesta.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(reclamos_txtRespuesta_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtRespuesta_string")

    reclamos_txtRespuesta_array_payload = ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtRespuesta':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}"]

    sc = spark.sparkContext
    reclamos_txtRespuesta_array_rdd = sc.parallelize(reclamos_txtRespuesta_array_payload)
    reclamos_txtRespuesta_array_json = spark.read.json(reclamos_txtRespuesta_array_rdd)
    reclamos_txtRespuesta_array_df = spark.read.schema(reclamos_txtRespuesta_array_json.schema).json(ar_path)
    reclamos_txtRespuesta_array_df = reclamos_txtRespuesta_array_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtRespuesta.value as txtRespuesta")
    reclamos_txtRespuesta_array_df = reclamos_txtRespuesta_array_df.filter(col("UniversalID").isNotNull())
    reclamos_txtRespuesta_array_df = reclamos_txtRespuesta_array_df.select("UniversalID","UniversalIDCompuesto", "dtFechaCreacion", array_join("txtRespuesta", '\r\n ').alias("txtRespuesta"))
    sqlContext.registerDataFrameAsTable(reclamos_txtRespuesta_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtRespuesta_array")
    
    reclamos_txtRespuesta_df = sqlContext.sql("SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, txtRespuesta\
                                               FROM \
                                                  Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtRespuesta_string \
                                               UNION \
                                               SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, txtRespuesta\
                                               FROM \
                                                  Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtRespuesta_array")
    sqlContext.registerDataFrameAsTable(reclamos_txtRespuesta_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txtRespuesta")
    
    """txtHistoria como String"""
    reclamos_txtHistoria_string_payload =  ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtHistoria':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_txtHistoria_string_rdd = sc.parallelize(reclamos_txtHistoria_string_payload)
    reclamos_txtHistoria_string_json = spark.read.json(reclamos_txtHistoria_string_rdd)
    reclamos_txtHistoria_string_df = spark.read.schema(reclamos_txtHistoria_string_json.schema).json(ar_path)
    reclamos_txtHistoria_string_df = reclamos_txtHistoria_string_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtHistoria.value as txtHistoria")
    reclamos_txtHistoria_string_df = reclamos_txtHistoria_string_df.filter(reclamos_txtHistoria_string_df.txtHistoria.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(reclamos_txtHistoria_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtHistoria_string")
    
    reclamos_txtHistoria_array_payload = ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtHistoria':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}"]

    sc = spark.sparkContext
    reclamos_txtHistoria_array_rdd = sc.parallelize(reclamos_txtHistoria_array_payload)
    reclamos_txtHistoria_array_json = spark.read.json(reclamos_txtHistoria_array_rdd)
    reclamos_txtHistoria_array_df = spark.read.schema(reclamos_txtHistoria_array_json.schema).json(ar_path)
    reclamos_txtHistoria_array_df = reclamos_txtHistoria_array_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtHistoria.value as txtHistoria")
    reclamos_txtHistoria_array_df = reclamos_txtHistoria_array_df.filter(col("UniversalID").isNotNull())
    reclamos_txtHistoria_array_df = reclamos_txtHistoria_array_df.select("UniversalID","UniversalIDCompuesto", "dtFechaCreacion", array_join("txtHistoria", '\r\n ').alias("txtHistoria"))
    sqlContext.registerDataFrameAsTable(reclamos_txtHistoria_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtHistoria_array")
    
    reclamos_txtHistoria_df = sqlContext.sql("SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion , txtHistoria \
                                              FROM \
                                                 Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtHistoria_string \
                                              UNION \
                                              SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion , txtHistoria \
                                              FROM \
                                                 Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtHistoria_array")
    sqlContext.registerDataFrameAsTable(reclamos_txtHistoria_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txtHistoria")
    
    ValorY = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_LOTUS_RECLAMOS_APP' LIMIT 1").first()["Valor1"]
    
    panel_principal_reclamos = sqlContext.sql("\
      SELECT \
      UniversalID, \
      TituloDB, \
      txtRadicado.value Radicado, \
      dtFechaCreacion.value FechaCreacion, \
      cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4)as int) anio, \
      cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes, \
      rdClienteRazon.value ClientetieneRazon, \
      numAbonoTemporal.value AbonoTemporal, \
      numAbonoDefinitivo.value AbonoDefinitivo, \
      txtTipoProducto.value Producto, \
      txtTipoReclamo.value TipoReclamo, \
      IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/reclamos/', UniversalID,'.zip'" + "), '') link, \
      translate(concat(concat(txtRadicado.value, ' - ', txtNumIdCliente.value, ' - ', rdClienteRazon.value, ' - ', numAbonoTemporal.value, ' - ', txtTipoProducto.value, ' - ', txtTipoReclamo.value, ' - ', numAbonoDefinitivo.value), ' - ', \
      upper(concat(txtRadicado.value, ' - ', txtNumIdCliente.value, ' - ', rdClienteRazon.value, ' - ', numAbonoTemporal.value, ' - ', txtTipoProducto.value, ' - ', txtTipoReclamo.value, ' - ', numAbonoDefinitivo.value)), ' - ',  \
      lower(concat(txtRadicado.value, ' - ', txtNumIdCliente.value, ' - ', rdClienteRazon.value, ' - ', numAbonoTemporal.value, ' - ', txtTipoProducto.value, ' - ', txtTipoReclamo.value, ' - ', numAbonoDefinitivo.value))),'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN') busqueda \
    FROM \
      Zdc_Lotus_Reclamos_TablaIntermedia")
    
    panel_detalle_reclamos = sqlContext.sql("\
    SELECT UniversalId, 'Radicación' AS vista, 1 as posicion, 'Autor' AS key, txtAutor.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Radicación' AS vista, 2 as posicion, 'Fecha de creación' AS key, dtFechaCreacion.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Radicación' AS vista, 3 as posicion, 'Sucursal Captura' AS key, TXTSUCCAPTURA.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Radicación' AS vista, 4 as posicion, 'Reportado Por' AS key, cbReportadoPor.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Radicación' AS vista, 5 as posicion, 'Origen del Reporte' AS key, cbOrigenReporte.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Radicación' AS vista, 6 as posicion, 'Responsable' AS key, txtResponsable.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Radicación' AS vista, 7 as posicion, 'Abono Definitivo' AS key, numAbonoDefinitivo.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Radicación' AS vista, 8 as posicion, 'Responsable del Error' AS key, cbResponsableError.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Radicación' AS vista, 9 as posicion, 'Nombre del Responsable del Error' AS key, txtNomRespError.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Radicación' AS vista, 10 as posicion, 'Cliente tiene la Razon' AS key, rdClienteRazon.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Cliente' AS vista, 11 as posicion, 'Número de identificación' AS key, txtNumIdCliente.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Cliente' AS vista, 12 as posicion, 'Nombre' AS key, txtNomCliente.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Cliente' AS vista, 13 as posicion, 'Segmento' AS key, txtSegmento.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Cliente' AS vista, 14 as posicion, 'Gerente de cuenta' AS key, txtGerenteCuenta.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Cliente' AS vista, 15 as posicion, 'Funcionario de la empresa' AS key, txtFuncionarioEmpresa.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Cliente' AS vista, 16 as posicion, 'Dirección de correspondencia' AS key, txtDireccionCliente.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Cliente' AS vista, 17 as posicion, 'Ciudad de correspondencia' AS key, cbCiudadCliente.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Cliente' AS vista, 18 as posicion, 'Código de ciudad' AS key, txtCodCiudadCliente.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Cliente' AS vista, 19 as posicion, 'Departamento de correspondencia' AS key, txtDeptoCliente.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Cliente' AS vista, 20 as posicion, 'Teléfono(s)' AS key, txtTelefonosCliente.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Cliente' AS vista, 21 as posicion, 'Celular' AS key, txtCelular.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Cliente' AS vista, 22 as posicion, 'Autoriza envío de información via Email' AS key, rdAutorizaEmail.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Cliente' AS vista, 23 as posicion, 'Email' AS key, txtEmailCliente.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Cliente' AS vista, 24 as posicion, 'Fax' AS key, txtFax.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Fecha de solución / vencimiento' AS vista, 25 as posicion, 'Tiempo de respuesta' AS key, numTiempoSlnRpta.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Fecha de solución / vencimiento' AS vista, 26 as posicion, 'Fecha de respuesta (dd/mm/aaaa)' AS key, dtFechaSlnRpta.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Fecha de solución / vencimiento' AS vista, 27 as posicion, 'Tiempo real de solución' AS key, numTiempoSolucionReal.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Fecha de solución / vencimiento' AS vista, 28 as posicion, 'Fecha real de solución (dd/mm/aaaa)' AS key, dtFechaSolucionReal.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Fecha de solución / vencimiento' AS vista, 29 as posicion, 'Tiempo estimado de solución' AS key, numTiempoSolucion.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 30 as posicion, 'Producto' AS key, txtTipoProducto.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 31 as posicion, 'Clasificación' AS key, txtClasificacion.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 32 as posicion, 'Tipo de reclamo' AS key, txtTipoReclamo.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 33 as posicion, 'Número de producto' AS key, txtNumProducto.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 34 as posicion, 'Código de compra' AS key, txtCodigoCompra.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 35 as posicion, 'Código de producto' AS key, txtCodigoProducto.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 36 as posicion, 'Producto afectado' AS key, cbProductoAfectado.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 37 as posicion, 'Número de tarjeta débito' AS key, txtNumTarjetaDebito.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 38 as posicion, 'Número de póliza' AS key, txtNumPoliza.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 39 as posicion, 'Número de encargo fiduciario' AS key, txtNumEncargoFiducia.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 40 as posicion, 'Sucursal de radicación del producto' AS key, cbSucursalRadicacion.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 41 as posicion, 'Tipo de póliza ' AS key, cbTipoPoliza.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 42 as posicion, 'Número de tarjeta / cuenta que se debita' AS key, txtNumTarjetaCtaDebita.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 43 as posicion, 'Ciudad de transacción / suceso' AS key, cbCiudadTransaccion.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 44 as posicion, 'Fecha de transacción' AS key, dtFechaTransaccion.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 45 as posicion, 'Sucursal / área de queja, felicitación o sugerencia' AS key, cbSucursalVarios.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 46 as posicion, 'Código único de establecimiento' AS key, txtCodigoEstablecimiento.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 47 as posicion, 'Valor total en pesos' AS key, numValorPesos.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 48 as posicion, 'Valor total en dólares' AS key, numValorDolares.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 49 as posicion, 'Comentario' AS key, txtComentario AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txtComentario \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 51 as posicion, 'Mensaje de ayuda' AS key, txtMensajeAyuda AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txtMensajeAyuda \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 50 as posicion, 'Observaciones (RappCollins)' AS key, txtObservaciones AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txtObservaciones \
    UNION ALL \
    SELECT UniversalId, 'Reclamo' AS vista, 52 as posicion, 'Soportes requeridos' AS key, txtSoportesRequeridos AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txtSoportesRequeridos \
    UNION ALL \
    SELECT UniversalId, 'Convenios' AS vista, 53 as posicion, 'Código del convenio	' AS key, txtCodConvenio.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Convenios' AS vista, 54 as posicion, 'Nombre de la empresa' AS key, txtNomEmpresa.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Convenios' AS vista, 55 as posicion, 'Nit de la empresa' AS key, txtNitEmpresa.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Convenios' AS vista, 56 as posicion, 'Código del canal' AS key, txtCodCanal.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Convenios' AS vista, 57 as posicion, 'Nombre del canal' AS key, txtNomCanal.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Soportes' AS vista, 58 as posicion, 'Soportes recibidos del cliente' AS key, txtSoportesRecibidos.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Soportes' AS vista, 59 as posicion, 'Cuáles no se han recibido' AS key, txtSoportesNoRecibidos.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Soportes' AS vista, 60 as posicion, 'Se recibieron todos los soportes?' AS key, rdTodosSoportes.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Pantallas' AS vista, 69 as posicion, 'Pantallas que documentan el reclamo - Anexos' AS key, rtPantallasReclamo.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Pantallas' AS vista, 70 as posicion, 'Pantallas que certifican la solución - Anexos' AS key, rtPantallasSolucion.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Pantallas' AS vista, 71 as posicion, 'Pantallas que documentan el reclamo - Texto	' AS key, txtPantallasReclamo AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txtPantallasReclamo \
    UNION ALL \
    SELECT UniversalId, 'Pantallas' AS vista, 72 as posicion, 'Pantallas que certifican la solución - Texto' AS key, txtPantallasSolucion AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txtPantallasSolucion \
    UNION ALL \
    SELECT UniversalId, 'Respuesta' AS vista, 73 as posicion, 'Respuesta (Respuesta)' AS key, txtRespuesta AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txtRespuesta \
    UNION ALL \
    SELECT UniversalId, 'Historia' AS vista, 74 as posicion, 'Comentarios y eventos' AS key, txtHistoria AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txtHistoria")
    
    return reclamos_df, panel_principal_reclamos, panel_detalle_reclamos
  except Exception as Error:
    print(Error, "\nError validando auditoria reclamos diseño nuevo")

# COMMAND ----------

def ft_reclamos_old(ar_path):
  try:
    reclamos_old_df = spark.read.schema(ft_schema_reclamos_old().schema).json(ar_path)
    sqlContext.registerDataFrameAsTable(reclamos_old_df, "Zdc_Lotus_Reclamos_old_TablaIntermedia")
    
    reclamos_txt_MensajeAyuda_string =  ["{'UniversalID':'string','txt_MensajeAyuda':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_txt_MensajeAyuda_string_rdd = sc.parallelize(reclamos_txt_MensajeAyuda_string)
    reclamos_txt_MensajeAyuda_string_json = spark.read.json(reclamos_txt_MensajeAyuda_string_rdd)
    reclamos_txt_MensajeAyuda_string_df = spark.read.schema(reclamos_txt_MensajeAyuda_string_json.schema).json(ar_path)
    reclamos_txt_MensajeAyuda_string_df = reclamos_txt_MensajeAyuda_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_MensajeAyuda.value as txt_MensajeAyuda")
    reclamos_txt_MensajeAyuda_string_df = reclamos_txt_MensajeAyuda_string_df.filter(reclamos_txt_MensajeAyuda_string_df.txt_MensajeAyuda.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(reclamos_txt_MensajeAyuda_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_MensajeAyuda_string")
    
    reclamos_txt_MensajeAyuda_array = ["{'UniversalID':'string','txt_MensajeAyuda':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_txt_MensajeAyuda_array_rdd = sc.parallelize(reclamos_txt_MensajeAyuda_array)
    reclamos_txt_MensajeAyuda_array_json = spark.read.json(reclamos_txt_MensajeAyuda_array_rdd)
    reclamos_txt_MensajeAyuda_array_df = spark.read.schema(reclamos_txt_MensajeAyuda_array_json.schema).json(ar_path)
    reclamos_txt_MensajeAyuda_array_df = reclamos_txt_MensajeAyuda_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_MensajeAyuda.value as txt_MensajeAyuda")
    reclamos_txt_MensajeAyuda_array_df = reclamos_txt_MensajeAyuda_array_df.filter(col("UniversalID").isNotNull())
    reclamos_txt_MensajeAyuda_array_df = reclamos_txt_MensajeAyuda_array_df.select("UniversalID","dat_FchCreac",array_join("txt_MensajeAyuda", '\r\n ').alias("txt_MensajeAyuda"))
    sqlContext.registerDataFrameAsTable(reclamos_txt_MensajeAyuda_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txt_MensajeAyuda_array")
    
    reclamos_txt_MensajeAyuda_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_MensajeAyuda_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txt_MensajeAyuda_array")
    sqlContext.registerDataFrameAsTable(reclamos_txt_MensajeAyuda_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txt_MensajeAyuda")

    reclamos_txt_MensajeSoportes_string =  ["{'UniversalID':'string','txt_MensajeSoportes':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_txt_MensajeSoportes_string_rdd = sc.parallelize(reclamos_txt_MensajeSoportes_string)
    reclamos_txt_MensajeSoportes_string_json = spark.read.json(reclamos_txt_MensajeSoportes_string_rdd)
    reclamos_txt_MensajeSoportes_string_df = spark.read.schema(reclamos_txt_MensajeSoportes_string_json.schema).json(ar_path)
    reclamos_txt_MensajeSoportes_string_df = reclamos_txt_MensajeSoportes_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_MensajeSoportes.value as txt_MensajeSoportes")
    reclamos_txt_MensajeSoportes_string_df = reclamos_txt_MensajeSoportes_string_df.filter(reclamos_txt_MensajeSoportes_string_df.txt_MensajeSoportes.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(reclamos_txt_MensajeSoportes_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_MensajeSoportes_string")

    reclamos_txt_MensajeSoportes_array = ["{'UniversalID':'string','txt_MensajeSoportes':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_txt_MensajeSoportes_array_rdd = sc.parallelize(reclamos_txt_MensajeSoportes_array)
    reclamos_txt_MensajeSoportes_array_json = spark.read.json(reclamos_txt_MensajeSoportes_array_rdd)
    reclamos_txt_MensajeSoportes_array_df = spark.read.schema(reclamos_txt_MensajeSoportes_array_json.schema).json(ar_path)
    reclamos_txt_MensajeSoportes_array_df = reclamos_txt_MensajeSoportes_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_MensajeSoportes.value as txt_MensajeSoportes")
    reclamos_txt_MensajeSoportes_array_df = reclamos_txt_MensajeSoportes_array_df.filter(col("UniversalID").isNotNull())
    reclamos_txt_MensajeSoportes_array_df = reclamos_txt_MensajeSoportes_array_df.select("UniversalID","dat_FchCreac",array_join("txt_MensajeSoportes", '\r\n ').alias("txt_MensajeSoportes"))
    sqlContext.registerDataFrameAsTable(reclamos_txt_MensajeSoportes_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txt_MensajeSoportes_array")
    
    reclamos_txt_MensajeSoportes_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_MensajeSoportes_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txt_MensajeSoportes_array")
    sqlContext.registerDataFrameAsTable(reclamos_txt_MensajeSoportes_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txt_MensajeSoportes")
    
    reclamos_rch_Pantallas_string =  ["{'UniversalID':'string','rch_Pantallas':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_rch_Pantallas_string_rdd = sc.parallelize(reclamos_rch_Pantallas_string)
    reclamos_rch_Pantallas_string_json = spark.read.json(reclamos_rch_Pantallas_string_rdd)
    reclamos_rch_Pantallas_string_df = spark.read.schema(reclamos_rch_Pantallas_string_json.schema).json(ar_path)
    reclamos_rch_Pantallas_string_df = reclamos_rch_Pantallas_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","rch_Pantallas.value as rch_Pantallas")
    reclamos_rch_Pantallas_string_df = reclamos_rch_Pantallas_string_df.filter(reclamos_rch_Pantallas_string_df.rch_Pantallas.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(reclamos_rch_Pantallas_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_Pantallas_string")
    
    reclamos_rch_Pantallas_array = ["{'UniversalID':'string','rch_Pantallas':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_rch_Pantallas_array_rdd = sc.parallelize(reclamos_rch_Pantallas_array)
    reclamos_rch_Pantallas_array_json = spark.read.json(reclamos_rch_Pantallas_array_rdd)
    reclamos_rch_Pantallas_array_df = spark.read.schema(reclamos_rch_Pantallas_array_json.schema).json(ar_path)
    reclamos_rch_Pantallas_array_df = reclamos_rch_Pantallas_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","rch_Pantallas.value as rch_Pantallas")
    reclamos_rch_Pantallas_array_df = reclamos_rch_Pantallas_array_df.filter(col("UniversalID").isNotNull())
    reclamos_rch_Pantallas_array_df = reclamos_rch_Pantallas_array_df.select("UniversalID","dat_FchCreac",array_join("rch_Pantallas", '\r\n ').alias("rch_Pantallas"))
    sqlContext.registerDataFrameAsTable(reclamos_rch_Pantallas_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_Pantallas_array") 
    reclamos_rch_Pantallas_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_Pantallas_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_Pantallas_array")
    sqlContext.registerDataFrameAsTable(reclamos_rch_Pantallas_df, "Zdc_Lotus_Reclamos_Tablaintermedia_rch_Pantallas")
    
    reclamos_rch_PantallaSln_string =  ["{'UniversalID':'string','rch_PantallaSln':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_rch_PantallaSln_string_rdd = sc.parallelize(reclamos_rch_PantallaSln_string)
    reclamos_rch_PantallaSln_string_json = spark.read.json(reclamos_rch_PantallaSln_string_rdd)
    reclamos_rch_PantallaSln_string_df = spark.read.schema(reclamos_rch_PantallaSln_string_json.schema).json(ar_path)
    reclamos_rch_PantallaSln_string_df = reclamos_rch_PantallaSln_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","rch_PantallaSln.value as rch_PantallaSln")
    reclamos_rch_PantallaSln_string_df = reclamos_rch_PantallaSln_string_df.filter(reclamos_rch_PantallaSln_string_df.rch_PantallaSln.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(reclamos_rch_PantallaSln_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_PantallaSln_string")
    
    reclamos_rch_PantallaSln_array = ["{'UniversalID':'string','rch_PantallaSln':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_rch_PantallaSln_array_rdd = sc.parallelize(reclamos_rch_PantallaSln_array)
    reclamos_rch_PantallaSln_array_json = spark.read.json(reclamos_rch_PantallaSln_array_rdd)
    reclamos_rch_PantallaSln_array_df = spark.read.schema(reclamos_rch_PantallaSln_array_json.schema).json(ar_path)
    reclamos_rch_PantallaSln_array_df = reclamos_rch_PantallaSln_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","rch_PantallaSln.value as rch_PantallaSln")
    reclamos_rch_PantallaSln_array_df = reclamos_rch_PantallaSln_array_df.filter(col("UniversalID").isNotNull())
    reclamos_rch_PantallaSln_array_df = reclamos_rch_PantallaSln_array_df.select("UniversalID","dat_FchCreac",array_join("rch_PantallaSln", '\r\n ').alias("rch_PantallaSln"))
    sqlContext.registerDataFrameAsTable(reclamos_rch_PantallaSln_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_PantallaSln_array")
    
    reclamos_rch_PantallaSln_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_PantallaSln_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_PantallaSln_array")
    sqlContext.registerDataFrameAsTable(reclamos_rch_PantallaSln_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_PantallaSln")
    
    reclamos_txt_Comentarios_string =  ["{'UniversalID':'string','txt_Comentarios':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_txt_Comentarios_string_rdd = sc.parallelize(reclamos_txt_Comentarios_string)
    reclamos_txt_Comentarios_string_json = spark.read.json(reclamos_txt_Comentarios_string_rdd)
    reclamos_txt_Comentarios_string_df = spark.read.schema(reclamos_txt_Comentarios_string_json.schema).json(ar_path)
    reclamos_txt_Comentarios_string_df = reclamos_txt_Comentarios_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Comentarios.value as txt_Comentarios")
    reclamos_txt_Comentarios_string_df = reclamos_txt_Comentarios_string_df.filter(reclamos_txt_Comentarios_string_df.txt_Comentarios.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(reclamos_txt_Comentarios_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Comentarios_string")
    
    reclamos_txt_Comentarios_array = ["{'UniversalID':'string','txt_Comentarios':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_txt_Comentarios_array_rdd = sc.parallelize(reclamos_txt_Comentarios_array)
    reclamos_txt_Comentarios_array_json = spark.read.json(reclamos_txt_Comentarios_array_rdd)
    reclamos_txt_Comentarios_array_df = spark.read.schema(reclamos_txt_Comentarios_array_json.schema).json(ar_path)
    reclamos_txt_Comentarios_array_df = reclamos_txt_Comentarios_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Comentarios.value as txt_Comentarios")
    reclamos_txt_Comentarios_array_df = reclamos_txt_Comentarios_array_df.filter(col("UniversalID").isNotNull())
    reclamos_txt_Comentarios_array_df = reclamos_txt_Comentarios_array_df.select("UniversalID","dat_FchCreac",array_join("txt_Comentarios", '\r\n ').alias("txt_Comentarios"))
    sqlContext.registerDataFrameAsTable(reclamos_txt_Comentarios_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Comentarios_array")
    
    reclamos_txt_Comentarios_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Comentarios_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Comentarios_array")
    sqlContext.registerDataFrameAsTable(reclamos_txt_Comentarios_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Comentarios")
    
    reclamos_txt_Historia_string =  ["{'UniversalID':'string','txt_Historia':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_txt_Historia_string_rdd = sc.parallelize(reclamos_txt_Historia_string)
    reclamos_txt_Historia_string_json = spark.read.json(reclamos_txt_Historia_string_rdd)
    reclamos_txt_Historia_string_df = spark.read.schema(reclamos_txt_Historia_string_json.schema).json(ar_path)
    reclamos_txt_Historia_string_df = reclamos_txt_Historia_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
    reclamos_txt_Historia_string_df = reclamos_txt_Historia_string_df.filter(reclamos_txt_Historia_string_df.txt_Historia.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(reclamos_txt_Historia_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Historia_string")
    
    reclamos_txt_Historia_array = ["{'UniversalID':'string','txt_Historia':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_txt_Historia_array_rdd = sc.parallelize(reclamos_txt_Historia_array)
    reclamos_txt_Historia_array_json = spark.read.json(reclamos_txt_Historia_array_rdd)
    reclamos_txt_Historia_array_df = spark.read.schema(reclamos_txt_Historia_array_json.schema).json(ar_path)
    reclamos_txt_Historia_array_df = reclamos_txt_Historia_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
    reclamos_txt_Historia_array_df = reclamos_txt_Historia_array_df.filter(col("UniversalID").isNotNull())
    reclamos_txt_Historia_array_df = reclamos_txt_Historia_array_df.select("UniversalID","dat_FchCreac",array_join("txt_Historia", '\r\n ').alias("txt_Historia"))
    sqlContext.registerDataFrameAsTable(reclamos_txt_Historia_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Historia_array")
    
    reclamos_txt_Historia_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Historia_array UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Historia_string")
    sqlContext.registerDataFrameAsTable(reclamos_txt_Historia_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Historia")
    
    reclamos_rch_RptaCliente_string =  ["{'UniversalID':'string','rch_RptaCliente':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_rch_RptaCliente_string_rdd = sc.parallelize(reclamos_rch_RptaCliente_string)
    reclamos_rch_RptaCliente_string_json = spark.read.json(reclamos_rch_RptaCliente_string_rdd)
    reclamos_rch_RptaCliente_string_df = spark.read.schema(reclamos_rch_RptaCliente_string_json.schema).json(ar_path)
    reclamos_rch_RptaCliente_string_df = reclamos_rch_RptaCliente_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","rch_RptaCliente.value as rch_RptaCliente")
    reclamos_rch_RptaCliente_string_df = reclamos_rch_RptaCliente_string_df.filter(reclamos_rch_RptaCliente_string_df.rch_RptaCliente.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(reclamos_rch_RptaCliente_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_RptaCliente_string")    
    reclamos_rch_RptaCliente_array = ["{'UniversalID':'string','rch_RptaCliente':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}"]

    sc = spark.sparkContext
    reclamos_rch_RptaCliente_array_rdd = sc.parallelize(reclamos_rch_RptaCliente_array)
    reclamos_rch_RptaCliente_array_json = spark.read.json(reclamos_rch_RptaCliente_array_rdd)
    reclamos_rch_RptaCliente_array_df = spark.read.schema(reclamos_rch_RptaCliente_array_json.schema).json(ar_path)
    reclamos_rch_RptaCliente_array_df = reclamos_rch_RptaCliente_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","rch_RptaCliente.value as rch_RptaCliente")
    reclamos_rch_RptaCliente_array_df = reclamos_rch_RptaCliente_array_df.filter(col("UniversalID").isNotNull())
    reclamos_rch_RptaCliente_array_df = reclamos_rch_RptaCliente_array_df.select("UniversalID","dat_FchCreac",array_join("rch_RptaCliente", '\r\n ').alias("rch_RptaCliente"))
    sqlContext.registerDataFrameAsTable(reclamos_rch_RptaCliente_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_RptaCliente_array")
    
    reclamos_rch_RptaCliente_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_RptaCliente_array UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_RptaCliente_string")
    sqlContext.registerDataFrameAsTable(reclamos_rch_RptaCliente_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_RptaCliente")
    
    reclamos_txt_NomOpTblM_string =  ["{'UniversalID':'string','txt_NomOpTblM':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_txt_NomOpTblM_string_rdd = sc.parallelize(reclamos_txt_NomOpTblM_string)
    reclamos_txt_NomOpTblM_string_json = spark.read.json(reclamos_txt_NomOpTblM_string_rdd)
    reclamos_txt_NomOpTblM_string_df = spark.read.schema(reclamos_txt_NomOpTblM_string_json.schema).json(ar_path)
    reclamos_txt_NomOpTblM_string_df = reclamos_txt_NomOpTblM_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_NomOpTblM.value as txt_NomOpTblM")
    reclamos_txt_NomOpTblM_string_df = reclamos_txt_NomOpTblM_string_df.filter(reclamos_txt_NomOpTblM_string_df.txt_NomOpTblM.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(reclamos_txt_NomOpTblM_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_NomOpTblM_string")
    
    reclamos_txt_NomOpTblM_array = ["{'UniversalID':'string','txt_NomOpTblM':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_txt_NomOpTblM_array_rdd = sc.parallelize(reclamos_txt_NomOpTblM_array)
    reclamos_txt_NomOpTblM_array_json = spark.read.json(reclamos_txt_NomOpTblM_array_rdd)
    reclamos_txt_NomOpTblM_array_df = spark.read.schema(reclamos_txt_NomOpTblM_array_json.schema).json(ar_path)
    reclamos_txt_NomOpTblM_array_df = reclamos_txt_NomOpTblM_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_NomOpTblM.value as txt_NomOpTblM")
    reclamos_txt_NomOpTblM_array_df = reclamos_txt_NomOpTblM_array_df.filter(col("UniversalID").isNotNull())
    reclamos_txt_NomOpTblM_array_df = reclamos_txt_NomOpTblM_array_df.select("UniversalID","dat_FchCreac",array_join("txt_NomOpTblM", '\r\n ').alias("txt_NomOpTblM"))
    sqlContext.registerDataFrameAsTable(reclamos_txt_NomOpTblM_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_NomOpTblM_array")
    
    reclamos_txt_NomOpTblM_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_NomOpTblM_array UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_NomOpTblM_string")
    sqlContext.registerDataFrameAsTable(reclamos_txt_NomOpTblM_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_NomOpTblM")
    
    reclamos_key_VlrsOpTblM_string =  ["{'UniversalID':'string','key_VlrsOpTblM':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_key_VlrsOpTblM_string_rdd = sc.parallelize(reclamos_key_VlrsOpTblM_string)
    reclamos_key_VlrsOpTblM_string_json = spark.read.json(reclamos_key_VlrsOpTblM_string_rdd)
    reclamos_key_VlrsOpTblM_string_df = spark.read.schema(reclamos_key_VlrsOpTblM_string_json.schema).json(ar_path)
    reclamos_key_VlrsOpTblM_string_df = reclamos_key_VlrsOpTblM_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","key_VlrsOpTblM.value as key_VlrsOpTblM")
    reclamos_key_VlrsOpTblM_string_df = reclamos_key_VlrsOpTblM_string_df.filter(reclamos_key_VlrsOpTblM_string_df.key_VlrsOpTblM.substr(0,1) != '[')
    sqlContext.registerDataFrameAsTable(reclamos_key_VlrsOpTblM_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_key_VlrsOpTblM_string")
    
    reclamos_key_VlrsOpTblM_array = ["{'UniversalID':'string','key_VlrsOpTblM':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}"]
    sc = spark.sparkContext
    reclamos_key_VlrsOpTblM_array_rdd = sc.parallelize(reclamos_key_VlrsOpTblM_array)
    reclamos_key_VlrsOpTblM_array_json = spark.read.json(reclamos_key_VlrsOpTblM_array_rdd)
    reclamos_key_VlrsOpTblM_array_df = spark.read.schema(reclamos_key_VlrsOpTblM_array_json.schema).json(ar_path)
    reclamos_key_VlrsOpTblM_array_df = reclamos_key_VlrsOpTblM_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","key_VlrsOpTblM.value as key_VlrsOpTblM")
    reclamos_key_VlrsOpTblM_array_df = reclamos_key_VlrsOpTblM_array_df.filter(col("UniversalID").isNotNull())
    reclamos_key_VlrsOpTblM_array_df = reclamos_key_VlrsOpTblM_array_df.select("UniversalID","dat_FchCreac",array_join("key_VlrsOpTblM", '\r\n ').alias("key_VlrsOpTblM"))
    sqlContext.registerDataFrameAsTable(reclamos_key_VlrsOpTblM_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_key_VlrsOpTblM_array")
    
    reclamos_key_VlrsOpTblM_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_key_VlrsOpTblM_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_key_VlrsOpTblM_array")
    sqlContext.registerDataFrameAsTable(reclamos_key_VlrsOpTblM_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_key_VlrsOpTblM")
    
    ValorY = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_LOTUS_RECLAMOS_APP' LIMIT 1").first()["Valor1"]

    sql_string = "SELECT \
        UniversalID \
        ,txt_CedNit.value CedNit \
        ,substring_index(dat_FchCreac.value, ' ', 1) fecha_solicitudes \
        ,key_Producto.value Producto \
        ,key_TipoReclamo.value TipoReclamo \
        ,num_AbonoDefinit.value AbonoDefinitivo \
        ,txt_NomCliente.value NomCliente \
        ,cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4)as int) anio \
        ,cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2)as int) mes \
        ,IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/reclamos-old/', UniversalID,'.zip'" + "), '') link \
        ,translate(concat(concat(txt_CedNit.value, ' - ' , key_TipoReclamo.value, ' - ', key_Producto.value, ' - ', num_AbonoDefinit.value, ' - ', txt_NomCliente.value), ' - ' \
        ,upper(concat(txt_CedNit.value, ' - ' , key_TipoReclamo.value, ' - ', key_Producto.value, ' - ', num_AbonoDefinit.value, ' - ', txt_NomCliente.value)), ' - ' \
        ,lower(concat(txt_CedNit.value, ' - ' , key_TipoReclamo.value, ' - ', key_Producto.value, ' - ', num_AbonoDefinit.value, ' - ', txt_NomCliente.value))),'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN') Busqueda \
    FROM \
      Zdc_Lotus_Reclamos_old_TablaIntermedia"
    panel_principal_reclamos_old = sqlContext.sql(sql_string)
    
    panel_detalle_reclamos_old = sqlContext.sql("\
    SELECT UniversalId, 'Reclamos' AS vista, 1 as posicion, 'Días Vencidos Totales' AS key, num_DV.value value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamos' AS vista, 2 as posicion, 'Días Vencidos de Soportes' AS key, num_DVPendiente.value value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamos' AS vista, 3 as posicion, 'Días Vencidos Mala radicación' AS key, num_DVMalRad.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamos' AS vista, 4 as posicion, 'Días Vencidos Pendiente Documentación' AS key, num_DVFraudeTrj.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamos' AS vista, 5 as posicion, 'Días Vencidos respuesta al CLiente' AS key, num_DVSlnado.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamos' AS vista, 6 as posicion, 'Días Vencidos Solucionado con Carta' AS key, num_DVSlnadoCarta.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamos' AS vista, 7 as posicion, 'Días Vencidos Pendiente Ajuste' AS key, num_DVPendAjuste.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamos' AS vista, 8 as posicion, 'Días Vencidos Verificación' AS key, num_DVVerif.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamos' AS vista, 9 as posicion, 'Días Vencidos Investigación' AS key, num_DVInvest.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamos' AS vista, 10 as posicion, 'Días Vencidos Redes' AS key, num_DVRedes.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamos' AS vista, 11 as posicion, 'Días Vencidos Bancos' AS key, num_DVBancos.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamos' AS vista, 12 as posicion, 'Días Vencidos Tramite Interno' AS key, num_DVTrmInterno.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamos' AS vista, 13 as posicion, 'Días Vencidos Solicita Respuesta Escrita' AS key, num_DVSolicRptaEsc.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamos' AS vista, 14 as posicion, 'Días Vencidos Cliente No Contactado' AS key, num_DVClteNoCont.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamos' AS vista, 15 as posicion, 'Días Vencidos Carta en Revisión' AS key, num_DVCartaEnRev.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamos' AS vista, 16 as posicion, 'Días Vencidos Susceptible Abono Temporal Abierto' AS key, num_DVAboTempA.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Reclamos' AS vista, 17 as posicion, 'Días Vencidos Susceptible Abono Temporal Pendiente' AS key, num_DVAboTempP.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, '' AS vista, 18 as posicion, 'Autor' AS key, txt_Autor.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, '' AS vista, 19 as posicion, 'Creador' AS key, txt_Creador.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, '' AS vista, 20 as posicion, 'Fecha de Creación' AS key, dat_FchCreac.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Radicación' AS vista, 21 as posicion, 'Sucursal de Captura' AS key, concat(key_NomSucCaptura.value, '\n', key_SucursalFilial.value) AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Radicación' AS vista, 22 as posicion, 'Region de Captura' AS key, txt_RgnCaptura.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Radicación' AS vista, 23 as posicion, 'Zona de Captura' AS key, txt_ZonaCaptura.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Radicación' AS vista, 24 as posicion, 'Reportado Por' AS key, key_ReportadoPor.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Radicación' AS vista, 25 as posicion, 'Origen del reporte' AS key, key_OrigenReporte.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Radicación' AS vista, 26 as posicion, 'Responsable' AS key, nom_Responsable_1.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Radicación' AS vista, 27 as posicion, 'Abono Temporal' AS key, num_AbonoTemp.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Radicación' AS vista, 28 as posicion, 'Abono Definitivo' AS key, num_AbonoDefinit.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Radicación' AS vista, 29 as posicion, 'Responsable del Error' AS key, key_ResponError.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Radicación' AS vista, 30 as posicion, 'Nombre del Responsable Error' AS key, txt_NomRespError.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Radicación' AS vista, 31 as posicion, 'El Cliente tiene la Razon?' AS key, key_ClienteRazon.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Radicación' AS vista, 32 as posicion, 'Causa de no Abono' AS key, key_CausaNoAbono.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Cierre del reclamo' AS vista, 33 as posicion, 'Usuario que Cierra' AS key, nom_UsuCierra.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Cierre del reclamo' AS vista, 34 as posicion, 'Tiempo Total (Creado - Cerrado)' AS key, num_DRecl.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Cierre del reclamo' AS vista, 35 as posicion, 'Medio de Respuesta al Cliente' AS key, key_MedioRpta.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 36 as posicion, 'Tipo de Documento' AS key, key_TipoDocumento.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 37 as posicion, 'Cedula/Nit' AS key, txt_CedNit.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 38 as posicion, 'Nombre' AS key, txt_NomCliente.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 39 as posicion, 'Tipo Cliente' AS key, txt_TipoCliente.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 40 as posicion, 'Funcionario de la Empresa' AS key, txt_FuncEmpresa.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 41 as posicion, 'SubSegmento' AS key, txt_SubSgmtoCliente.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 42 as posicion, 'Es Cliente Gerenciado?' AS key, CASE \
        WHEN LENGTH(COALESCE(txt_clteger.value,'')) = 0 AND LENGTH(txt_CedNit.value) = 0 THEN '' \
        WHEN LENGTH(COALESCE(txt_clteger.value,'')) = 0 AND LENGTH(txt_CedNit.value) > 0 THEN 'No' \
        WHEN UPPER(txt_clteger.value) = 'G' AND LENGTH(txt_CedNit.value) >= 0  THEN 'Si' \
        WHEN UPPER(txt_clteger.value) = 'N' AND LENGTH(txt_CedNit.value) >= 0  THEN 'No' \
      END AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 43 as posicion, 'Gerente de Cuenta' AS key, txt_GteCuenta.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 44 as posicion, 'Es Cliente Colombia?' AS key, CASE \
        WHEN LENGTH(COALESCE(txt_ClteColombia.value,'')) = 0 AND LENGTH(txt_CedNit.value) = 0 THEN '' \
        WHEN LENGTH(COALESCE(txt_ClteColombia.value,'')) = 0 AND LENGTH(txt_CedNit.value) > 0 THEN 'No' \
        WHEN UPPER(txt_ClteColombia.value) = 'CC' AND LENGTH(txt_CedNit.value) >= 0  THEN 'Si' \
        WHEN UPPER(txt_ClteColombia.value) = 'N' AND LENGTH(txt_CedNit.value) >= 0  THEN 'No' \
      END AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 45 as posicion, 'Ciudad de Correspondencia' AS key, concat(txt_CiudadCorr.value, ' - ', txt_DescCiudadCorr.value) AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 46 as posicion, 'Dpto Correspondencia' AS key, txt_DptoCorr.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 47 as posicion, 'Dirección Correspondencia' AS key, txt_DirCorr.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 48 as posicion, 'País' AS key, txt_Pais.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 49 as posicion, 'Estado' AS key, txt_Estado.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 50 as posicion, 'Dirección Postal' AS key, txt_DirPostal.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 51 as posicion, 'Apartado Aereo (P.O BOX)' AS key, txt_ApartAereo.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 52 as posicion, 'Identificador Dirección' AS key, txt_IdDireccion.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 53 as posicion, 'Como Desea Recibir Respuesta a su Reclamo?' AS key, key_RptaReclamo.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 55 as posicion, 'Telefonos' AS key, txt_TeleCliente.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 56 as posicion, 'Fax' AS key, txt_FaxCliente.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Cliente' AS vista, 57 as posicion, 'E-mail' AS key, txt_EmailCliente.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Responsable del Seguimiento al reclamo' AS vista, 58 as posicion, 'Nombre del Responsable' AS key, nom_respGer.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Responsable del Seguimiento al reclamo' AS vista, 59 as posicion, 'Telefono Directo del Responsable' AS key, txt_TelRespGer.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Fechas de la Solucion/Vencimiento' AS vista, 60 as posicion, 'Tiempo Estimado de Solución' AS key, num_TpoEstSol.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Fechas de la Solucion/Vencimiento' AS vista, 61 as posicion, 'Fecha Aprox de Solución' AS key, dat_FchAproxSol.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Fechas de la Solucion/Vencimiento' AS vista, 62 as posicion, 'Tiempo Real de Solución' AS key, num_DAbierto.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos de Fechas de la Solucion/Vencimiento' AS vista, 63 as posicion, 'Fecha real de Solución' AS key, dat_Solucionado.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 64 as posicion, 'Compañia' AS key, txt_Cia.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 65 as posicion, 'Producto' AS key, key_Producto.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 66 as posicion, 'Clasificación' AS key, key_TipoTarjeta.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 67 as posicion, 'Tipo Reclamo' AS key, key_TipoReclamo.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 68 as posicion, 'Número de Producto' AS key,concat(key_Prefijo.value, ' - ', txt_NumProducto.value) AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 69 as posicion, 'Codigo de Compra' AS key, txt_NroCompraRapp.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 70 as posicion, 'Código de Producto' AS key, txt_NroProductoRapp.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 71 as posicion, 'Sub-Proceso Dueño del Reclamo' AS key, txt_SubprocRecl.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 72 as posicion, 'Número de Tarjeta Debito' AS key, txt_NumTarjDeb.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 73 as posicion, 'Número de Poliza' AS key, txt_NumPoliza.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 74 as posicion, 'Número de Encargo Fiduciario' AS key, txt_NumEncargo.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 75 as posicion, 'Sucursal de Radicacion' AS key, key_NomSucRadica.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 76 as posicion, 'Tipo de Poliza' AS key, txt_TipoPoliza.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 77 as posicion, 'Número de la Tarjeta/Cuenta que se Debita' AS key, txt_NumTjtaCtaDeb.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 78 as posicion, 'Ciudad de Transacción/Suceso' AS key, key_CiudadTransa.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 79 as posicion, 'Fecha Transacción' AS key, dat_FchTransac.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 80 as posicion, 'Suc/Area de Queja, Felicitacion o Sugerencia' AS key, key_NomSucVarios.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 81 as posicion, 'Código Unico de Establecimiento' AS key, txt_CodEstab.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 82 as posicion, 'Número Transacciones' AS key, key_NumTransa.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 85 as posicion, 'Mensaje de Ayuda' AS key, txt_MensajeAyuda AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txt_MensajeAyuda \
    UNION ALL \
    SELECT UniversalId, 'Datos del Reclamo' AS vista, 86 as posicion, 'Soportes requeridos' AS key, txt_MensajeSoportes AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txt_MensajeSoportes \
    UNION ALL \
    SELECT UniversalId, 'Observaciones' AS vista, 87 as posicion, '' AS key, txt_ComentRapp.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Soportes Recibidos' AS vista, 88 as posicion, 'Soportes recibidos del Cliente' AS key, txt_SopRecCte.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Soportes Recibidos' AS vista, 89 as posicion, 'Cuales no se han Recibido' AS key, txt_SopNoRec.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Soportes Recibidos' AS vista, 90 as posicion, 'Se recibieron todos los Soportes?' AS key, key_TodosSoportes.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
    UNION ALL \
    SELECT UniversalId, 'Pantallas que Documentan el Reclamo' AS vista, 91 as posicion, '' AS key, rch_Pantallas AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_rch_Pantallas \
    UNION ALL \
    SELECT UniversalId, 'Pantallas que Certifican la Solución' AS vista, 92 as posicion, '' AS key, rch_PantallaSln AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_PantallaSln \
    UNION ALL \
    SELECT UniversalId, 'Comentarios' AS vista, 93 as posicion, '' AS key, txt_Comentarios AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Comentarios \
    UNION ALL \
    SELECT UniversalId, 'Historia' AS vista, 94 as posicion, '' AS key, txt_Historia AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Historia \
    UNION ALL \
    SELECT UniversalId, 'Respuesta al Cliente' AS vista, 95 as posicion, '' AS key, rch_RptaCliente AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_RptaCliente \
    UNION ALL \
    SELECT UniversalId, 'Datos Adicionales del Reclamo' AS vista, 96 as posicion, '' AS key, txt_NomOpTblM AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_NomOpTblM \
    UNION ALL \
    SELECT UniversalId, 'Datos Adicionales del Reclamo' AS vista, 97 as posicion, '' AS key, key_VlrsOpTblM AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_key_VlrsOpTblM")
    
    return reclamos_old_df, panel_principal_reclamos_old, panel_detalle_reclamos_old
  except Exception as Error:
    print(Error, "\nError validando auditoria reclamos diseño viejo")

# COMMAND ----------

try:
  path, aplication = ft_auditoria().split("|")
  if aplication == "cayman":
     df_registros, panel_principal, panel_detalle = ft_cayman(path)
  elif aplication == "comex":
    df_registros, panel_principal, panel_detalle = ft_comex(path)
  elif aplication == "pedidos.pedidos":
    df_registros, panel_principal, panel_detalle = ft_pedidos(path)
  elif aplication == "pedidos.subpedidos":
    df_registros, panel_principal, panel_detalle = ft_subpedidos(path) 
  elif aplication == "reclamos.primercontacto":
    df_registros, panel_principal, panel_detalle = ft_primercontacto(path)
  elif aplication == "reclamos.primercontacto-old":
    df_registros, panel_principal, panel_detalle = ft_primercontacto_old(path)
  elif aplication == "reclamos.reclamos":
    df_registros, panel_principal, panel_detalle = ft_reclamos(path)
  elif aplication == "reclamos.reclamos-old":
    df_registros, panel_principal, panel_detalle = ft_reclamos_old(path)
  elif aplication == "reclamos.subreclamos":
    df_registros, panel_principal, panel_detalle = ft_subreclamos(path)
  elif aplication == "reclamos.subreclamos-old":
    df_registros, panel_principal, panel_detalle = subreclamos_old(path)
  elif aplication == "reclasificados":
    df_registros, panel_principal, panel_detalle = ft_reclasificados(path)
  elif aplication == "solicitudes":
    df_registros, panel_principal, panel_detalle = ft_solicitudes(path)
  elif aplication == "requerimientos-legales":
    df_registros, panel_principal, panel_detalle = ft_requerimientos(path)
  else:
    print("No registro ninguna aplicacion")
except Exception as Error:
  print(Error)
