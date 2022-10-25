# Databricks notebook source
# MAGIC %md
# MAGIC #NOTIFICADOR DE ESTADOS CARGA VÍA EMAIL

# COMMAND ----------

# DBTITLE 1,Librerias
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
from pyspark.sql.functions import *
from ast import literal_eval

# COMMAND ----------

# DBTITLE 1,Generar HTML Correo
"""
Descripción: Plantilla HTML - Notificación Correo Electronico - Data Factory.
Parametros: str title = "", str parrafo = "", Dataframe Pandas df_tabla = None.
Fecha Creacion: 19/05/2020.
Autor: Juan Escobar, Diego Velasco.
"""

def generateHtmlDynamicEmail(title = "", parrafo = "", df_tabla = {}):
  page_html = "<html>"
  head_html = """\
                    <head>
                    <style>
                      table {
                        width:85%;
                      }      
                      table, th, td {
                        border: 1px solid black;
                        border-collapse: collapse;
                      }
                      th, td {
                        padding: 5px;
                        text-align: left;
                      }
                    </style>
                    </head>
               """
  
  body_html = "<body>"

  body_title_html = "<h2>[MSG_H2]</h2>"
  body_title_html = body_title_html.replace(body_title_html, "<h2>" + title + "</h2>" ) if title != '' else ''

  body_paragraph_html = "<p>[MSG_PARRAFO_1]</p><br/>"
  body_paragraph_html = body_paragraph_html.replace(body_paragraph_html, "<p>" + parrafo + "</p><br/>") if parrafo != '' else ''

  #Replace Table
  table_dynamic_html = '' if df_tabla.empty else createTableHtml(df_tabla)

  footer_html = "</body></html>"

  email_html = page_html + head_html + body_html + body_title_html + body_paragraph_html + table_dynamic_html + footer_html   
   
  return email_html


# COMMAND ----------

# DBTITLE 1,Crear Tabla Dinámica HTML - Dataframe
"""
Decripcion: crea una tabla HTML dinamica en base a un objeto Dataframe Pandas
Parametros: Dataframe.pandas
Fecha CVreación: 19/05/2020
Autor: Juan Escobar, Diego Velasco.
"""

def createTableHtml(df):

  table_html = "<table>"
  table_headers = "<tr>"
  table_body = ""
  table_footer = "</table>"
  current_row = "<tr>" 
  current_row_end = "</tr>"
  
  for col_name in df.columns: 
    table_headers += "<th>" + str(col_name) + "</th>"

  table_headers += "</tr>"

  for indice_fila, fila in df.iterrows():
    table_body += current_row
    for col_name in df.columns:
      table_body += "<td>" + str(fila[col_name]) + "</td>"
    table_body += current_row_end

  table_html += table_headers + table_body + table_footer
  return table_html

# COMMAND ----------

# DBTITLE 1,Envió de correo electrónico
def sendMessage(prm_df_contenido, prm_subject, prm_titulo, prm_paragraph):
    SERVER = "smtp-mail.outlook.com"
    PORT = 25
    FROM = "dvelasco@bancolombia.com.co"
    TO = ["juadaves@bancolombia.com.co", "dvelasco@bancolombia.com.co"] # must be a list
    
    df = prm_df_contenido
    message = MIMEMultipart('alternative')
    message['Subject'] = prm_subject
    msg_html = generateHtmlDynamicEmail(prm_titulo, prm_paragraph, prm_df_contenido)
    
    print(msg_html)
    
    # Prepare actual message, Record the MIME types of both parts - text/plain and text/html.
    msg_email = MIMEText(msg_html, 'html')

    # Attach parts into message container, According to RFC 2046, the last part of a multipart message, in this case, the HTML message, is best and preferred.
    message.attach(msg_email)
    
    # Send the mail    
    try:     
      server = smtplib.SMTP(SERVER)
      server.connect(SERVER,PORT)
      server.ehlo()
      server.starttls()
      server.ehlo()
      server.login("mi_usuario", "mi_password")
      server.sendmail(FROM, TO, message.as_string())
      server.quit()
      print ('successfully sent the mail')
    except NameError:
      print ('failed to send mail: ', NameError)

# COMMAND ----------

# DBTITLE 1,Inicializar parámetros de entrada
def init_sendMessage(df, prm_content_msg = {}):
  subject = prm_content_msg['subject']
  titulo = prm_content_msg['titulo']
  paragraph = prm_content_msg['paragraph']
  sendMessage(prm_df_contenido = df, prm_subject = subject,  prm_titulo = titulo, prm_paragraph = paragraph)
