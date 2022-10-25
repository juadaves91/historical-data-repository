# Databricks notebook source
# DBTITLE 1,Mapeo Storage Account Utilidades
dbutils.fs.mount(
  source = "wasbs://utilidades@historicosdesasac01.blob.core.windows.net",
  mount_point = "/mnt/utilidades",
  extra_configs = {"fs.azure.account.key.historicosdesasac01.blob.core.windows.net":dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "SecretStorageAccountKey")}
)

# COMMAND ----------

# DBTITLE 1,Mapeo Storage Account Cayman
dbutils.fs.mount(
  source = "wasbs://lotus-cayman@historicosdesasac01.blob.core.windows.net",
  mount_point = "/mnt/lotus-cayman-storage",
  extra_configs = {"fs.azure.account.key.historicosdesasac01.blob.core.windows.net":dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "SecretStorageAccountKey")}
)

# COMMAND ----------

# DBTITLE 1,Mapeo Storage Account Reclamos
dbutils.fs.mount(
  source = "wasbs://lotus-reclamos@historicosdesasac01.blob.core.windows.net",
  mount_point = "/mnt/lotus-reclamos-storage",
  extra_configs = {"fs.azure.account.key.historicosdesasac01.blob.core.windows.net":dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "SecretStorageAccountKey")}
)

# COMMAND ----------

# DBTITLE 1,Mapeo Storage Account Requerimientos Legales
dbutils.fs.mount(
  source = "wasbs://lotus-requerimientos-legales@historicosdesasac01.blob.core.windows.net",
  mount_point = "/mnt/lotus-requerimientos-legales-storage",
  extra_configs = {"fs.azure.account.key.historicosdesasac01.blob.core.windows.net":dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "SecretStorageAccountKey")}
)

# COMMAND ----------

# DBTITLE 1,Mapeo Storage Account Reclasificados
dbutils.fs.mount(
  source = "wasbs://lotus-reclasificados@historicosdesasac01.blob.core.windows.net",
  mount_point = "/mnt/lotus-reclasificados-storage",
  extra_configs = {"fs.azure.account.key.historicosdesasac01.blob.core.windows.net":dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "SecretStorageAccountKey")}
)

# COMMAND ----------

# DBTITLE 1,Mapeo Storage Account Comex
dbutils.fs.mount(
  source = "wasbs://lotus-comex@historicosdesasac01.blob.core.windows.net",
  mount_point = "/mnt/lotus-comex-storage",
  extra_configs = {"fs.azure.account.key.historicosdesasac01.blob.core.windows.net":dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "SecretStorageAccountKey")}
)

# COMMAND ----------

# DBTITLE 1,Mapeo Storage Account Solicitudes
dbutils.fs.mount(
  source = "wasbs://lotus-solicitudes@historicosdesasac01.blob.core.windows.net",
  mount_point = "/mnt/lotus-solicitudes-storage",
  extra_configs = {"fs.azure.account.key.historicosdesasac01.blob.core.windows.net":dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "SecretStorageAccountKey")}
)

# COMMAND ----------

# DBTITLE 1,Mapeo Storage Account Pedidos
dbutils.fs.mount(
  source = "wasbs://lotus-pedidos@historicosdesasac01.blob.core.windows.net",
  mount_point = "/mnt/lotus-pedidos-storage",
  extra_configs = {"fs.azure.account.key.historicosdesasac01.blob.core.windows.net":dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "SecretStorageAccountKey")}
)

# COMMAND ----------

# DBTITLE 1,Mapeo Storage Account Igconbank
dbutils.fs.mount(
  source = "wasbs://app-igconbank@historicosdesasac01.blob.core.windows.net",
  mount_point = "/mnt/app-igconbank-storage",
  extra_configs = {"fs.azure.account.key.historicosdesasac01.blob.core.windows.net":dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "SecretStorageAccountKey")}
)

# COMMAND ----------

# DBTITLE 1,Mapeo Storage Account Semi H
dbutils.fs.mount(
  source = "wasbs://app-semih@historicosdesasac01.blob.core.windows.net",
  mount_point = "/mnt/app-semih-storage",
  extra_configs = {"fs.azure.account.key.historicosdesasac01.blob.core.windows.net":dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "SecretStorageAccountKey")}
)

# COMMAND ----------

# DBTITLE 1,Mapeo Storage Account Safyr
dbutils.fs.mount(
  source = "wasbs://app-safyr@historicosdesasac01.blob.core.windows.net",
  mount_point = "/mnt/app-safyr-storage",
  extra_configs = {"fs.azure.account.key.historicosdesasac01.blob.core.windows.net":dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "SecretStorageAccountKey")}
)

# COMMAND ----------

# DBTITLE 1,Mapeo Storage Account Helisa
dbutils.fs.mount(
  source = "wasbs://app-helisa@historicosdesasac01.blob.core.windows.net",
  mount_point = "/mnt/app-helisa-storage",
  extra_configs = {"fs.azure.account.key.historicosdesasac01.blob.core.windows.net":dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "SecretStorageAccountKey")}
)

# COMMAND ----------

# DBTITLE 1,Mapeo Storage Account Aldon
dbutils.fs.mount(
  source = "wasbs://app-aldon@historicosdesasac01.blob.core.windows.net",
  mount_point = "/mnt/app-aldon-storage",
  extra_configs = {"fs.azure.account.key.historicosdesasac01.blob.core.windows.net":dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "SecretStorageAccountKey")}
)

# COMMAND ----------

# DBTITLE 1,Mapeo Storage Account Mailpoint
dbutils.fs.mount(
  source = "wasbs://app-mailpoint@historicosdesasac01.blob.core.windows.net",
  mount_point = "/mnt/app-mailpoint-storage",
  extra_configs = {"fs.azure.account.key.historicosdesasac01.blob.core.windows.net":dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "SecretStorageAccountKey")}
)

# COMMAND ----------

# DBTITLE 1,Mapeo Data Lake Cayman
configs = {
  "dfs.adls.oauth2.access.token.provider.type" : "ClientCredential",
  "dfs.adls.oauth2.client.id" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "usernamecayman"),
  "dfs.adls.oauth2.credential" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "passwordcayman"),
  "dfs.adls.oauth2.refresh.url" : "https://login.microsoftonline.com/428f4e2e-13bf-4884-b364-02ef9af41a1d/oauth2/token"
}

dbutils.fs.mount(
  source = "adl://historicosdesadla01.azuredatalakestore.net/lotus-cayman",
  mount_point = "/mnt/lotus-cayman",
  extra_configs = configs
)

# COMMAND ----------

# DBTITLE 1,Mapeo Data Lake Reclamos
configs = {
  "dfs.adls.oauth2.access.token.provider.type" : "ClientCredential",
  "dfs.adls.oauth2.client.id" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "usernamereclamos"),
  "dfs.adls.oauth2.credential" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "passwordreclamos"),
  "dfs.adls.oauth2.refresh.url" : "https://login.microsoftonline.com/428f4e2e-13bf-4884-b364-02ef9af41a1d/oauth2/token"
}

dbutils.fs.mount(
  source = "adl://historicosdesadla01.azuredatalakestore.net/lotus-reclamos",
  mount_point = "/mnt/lotus-reclamos",
  extra_configs = configs
)

# COMMAND ----------

# DBTITLE 1,Mapeo Data Lake Requerimientos Legales
configs = {
  "dfs.adls.oauth2.access.token.provider.type" : "ClientCredential",
  "dfs.adls.oauth2.client.id" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "usernamereqlegales"),
  "dfs.adls.oauth2.credential" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "passwordreqlegales"),
  "dfs.adls.oauth2.refresh.url" : "https://login.microsoftonline.com/428f4e2e-13bf-4884-b364-02ef9af41a1d/oauth2/token"
}

dbutils.fs.mount(
  source = "adl://historicosdesadla01.azuredatalakestore.net/lotus-requerimientos-legales",
  mount_point = "/mnt/lotus-requerimientos-legales",
  extra_configs = configs
)

# COMMAND ----------

# DBTITLE 1,Mapeo Data Lake Reclasificados
configs = {
  "dfs.adls.oauth2.access.token.provider.type" : "ClientCredential",
  "dfs.adls.oauth2.client.id" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "usernamereclasificados"),
  "dfs.adls.oauth2.credential" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "passwordreclasificados"),
  "dfs.adls.oauth2.refresh.url" : "https://login.microsoftonline.com/428f4e2e-13bf-4884-b364-02ef9af41a1d/oauth2/token"
}

dbutils.fs.mount(
  source = "adl://historicosdesadla01.azuredatalakestore.net/lotus-reclasificados",
  mount_point = "/mnt/lotus-reclasificados",
  extra_configs = configs
)

# COMMAND ----------

# DBTITLE 1,Mapeo Data Lake Comex
configs = {
  "dfs.adls.oauth2.access.token.provider.type" : "ClientCredential",
  "dfs.adls.oauth2.client.id" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "usernamecomex"),
  "dfs.adls.oauth2.credential" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "passwordcomex"),
  "dfs.adls.oauth2.refresh.url" : "https://login.microsoftonline.com/428f4e2e-13bf-4884-b364-02ef9af41a1d/oauth2/token"
}

dbutils.fs.mount(
  source = "adl://historicosdesadla01.azuredatalakestore.net/lotus-comex",
  mount_point = "/mnt/lotus-comex",
  extra_configs = configs
)

# COMMAND ----------

# DBTITLE 1,Mapeo Data Lake Solicitudes
configs = {
  "dfs.adls.oauth2.access.token.provider.type" : "ClientCredential",
  "dfs.adls.oauth2.client.id" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "usernamesolicitudes"),
  "dfs.adls.oauth2.credential" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "passwordsolicitudes"),
  "dfs.adls.oauth2.refresh.url" : "https://login.microsoftonline.com/428f4e2e-13bf-4884-b364-02ef9af41a1d/oauth2/token"
}

dbutils.fs.mount(
  source = "adl://historicosdesadla01.azuredatalakestore.net/lotus-solicitudes",
  mount_point = "/mnt/lotus-solicitudes",
  extra_configs = configs
)

# COMMAND ----------

# DBTITLE 1,Mapeo Data Lake Pedidos
configs = {
  "dfs.adls.oauth2.access.token.provider.type" : "ClientCredential",
  "dfs.adls.oauth2.client.id" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "usernamepedidos"),
  "dfs.adls.oauth2.credential" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "passwordpedidos"),
  "dfs.adls.oauth2.refresh.url" : "https://login.microsoftonline.com/428f4e2e-13bf-4884-b364-02ef9af41a1d/oauth2/token"
}
 
dbutils.fs.mount(
  source = "adl://historicosdesadla01.azuredatalakestore.net/lotus-pedidos",
  mount_point = "/mnt/lotus-pedidos",
  extra_configs = configs
)

# COMMAND ----------

# DBTITLE 1,Mapeo Data Lake Auditoria
configs = {
  "dfs.adls.oauth2.access.token.provider.type" : "ClientCredential",
  "dfs.adls.oauth2.client.id" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "usernamerdhauditoria"),
  "dfs.adls.oauth2.credential" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "passwordrdhauditoria"),
  "dfs.adls.oauth2.refresh.url" : "https://login.microsoftonline.com/428f4e2e-13bf-4884-b364-02ef9af41a1d/oauth2/token"
}


dbutils.fs.mount(
  source = "adl://historicosdesadla01.azuredatalakestore.net/rdh-auditoria",
  mount_point = "/mnt/rdh-auditoria",
  extra_configs = configs
)

# COMMAND ----------

# DBTITLE 1,Mapeo Igconbank
configs = {
  "dfs.adls.oauth2.access.token.provider.type" : "ClientCredential",
  "dfs.adls.oauth2.client.id" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "usernameigconbank"),
  "dfs.adls.oauth2.credential" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "passwordigconbank"),
  "dfs.adls.oauth2.refresh.url" : "https://login.microsoftonline.com/428f4e2e-13bf-4884-b364-02ef9af41a1d/oauth2/token"
}
 
dbutils.fs.mount(
  source = "adl://historicosdesadla01.azuredatalakestore.net/app-igconbank",
  mount_point = "/mnt/app-igconbank",
  extra_configs = configs
)

# COMMAND ----------

# DBTITLE 1,Mapeo Data Lake SemiH
configs = {
  "dfs.adls.oauth2.access.token.provider.type" : "ClientCredential",
  "dfs.adls.oauth2.client.id" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "usernamesemih"),
  "dfs.adls.oauth2.credential" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "passwordsemih"),
  "dfs.adls.oauth2.refresh.url" : "https://login.microsoftonline.com/428f4e2e-13bf-4884-b364-02ef9af41a1d/oauth2/token"
}
 
dbutils.fs.mount(
  source = "adl://historicosdesadla01.azuredatalakestore.net/app-semih",
  mount_point = "/mnt/app-semih",
  extra_configs = configs
)

# COMMAND ----------

# DBTITLE 1,Mapeo Data Lake Safyr
configs = {
  "dfs.adls.oauth2.access.token.provider.type" : "ClientCredential",
  "dfs.adls.oauth2.client.id" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "usernamesafyr"),
  "dfs.adls.oauth2.credential" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "passwordsafyr"),
  "dfs.adls.oauth2.refresh.url" : "https://login.microsoftonline.com/428f4e2e-13bf-4884-b364-02ef9af41a1d/oauth2/token"
}
 
dbutils.fs.mount(
  source = "adl://historicosdesadla01.azuredatalakestore.net/app-safyr",
  mount_point = "/mnt/app-safyr",
  extra_configs = configs
)

# COMMAND ----------

# DBTITLE 1,Mapeo Data Lake Helisa
configs = {
  "dfs.adls.oauth2.access.token.provider.type" : "ClientCredential",
  "dfs.adls.oauth2.client.id" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "usernamehelisa"),
  "dfs.adls.oauth2.credential" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "passwordhelisa"),
  "dfs.adls.oauth2.refresh.url" : "https://login.microsoftonline.com/428f4e2e-13bf-4884-b364-02ef9af41a1d/oauth2/token"
}
 
dbutils.fs.mount(
  source = "adl://historicosdesadla01.azuredatalakestore.net/app-helisa",
  mount_point = "/mnt/app-helisa",
  extra_configs = configs
)

# COMMAND ----------

# DBTITLE 1,Mapeo Data Lake Aldon
configs = {
  "dfs.adls.oauth2.access.token.provider.type" : "ClientCredential",
  "dfs.adls.oauth2.client.id" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "usernamealdon"),
  "dfs.adls.oauth2.credential" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "passwordaldon"),
  "dfs.adls.oauth2.refresh.url" : "https://login.microsoftonline.com/428f4e2e-13bf-4884-b364-02ef9af41a1d/oauth2/token"
}
 
dbutils.fs.mount(
  source = "adl://historicosdesadla01.azuredatalakestore.net/app-aldon",
  mount_point = "/mnt/app-aldon",
  extra_configs = configs
)


# COMMAND ----------

# DBTITLE 1,Mapeo Data Lake Mailpoint
configs = {
  "dfs.adls.oauth2.access.token.provider.type" : "ClientCredential",
  "dfs.adls.oauth2.client.id" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "usernamemailpoint"),
  "dfs.adls.oauth2.credential" : dbutils.secrets.get(scope = "historicos-desa-kva-01", key = "passwordmailpoint"),
  "dfs.adls.oauth2.refresh.url" : "https://login.microsoftonline.com/428f4e2e-13bf-4884-b364-02ef9af41a1d/oauth2/token"
}
 
dbutils.fs.mount(
  source = "adl://historicosdesadla01.azuredatalakestore.net/app-mailpoint",
  mount_point = "/mnt/app-mailpoint",
  extra_configs = configs
)