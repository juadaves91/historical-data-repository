# Databricks notebook source
# DBTITLE 1,Get_Attachements_Power_Shell
# Obtiene los anexos del Blob Storage, mediante Power Shell y los descarga a un archivo .TXT con las columnas sub_folder, Name, Length

# $containers = "lotus-cayman","lotus-reclasificados","lotus-reclamos","lotus-pedidos","lotus-solicitudes","lotus-requerimientos-legales","lotus-comex"
# $resourceGroup = "historicos-produccion-rg-01"
# $storageAccountName = "historicosprodsac01"

# Write-Host "Iniciamos"

# foreach ($container in $containers) {
#     $date = Get-Date -Format "dd/MM/yyyy HH:mm"
#     $containerName = $container
    
#     $length_container = $container.Length
#     $aplicacion = $container.Substring(6, $length_container - 6)

#     Write-Host "Start" $aplicacion $date

#     $storageAccount = Get-AzStorageAccount -Name $storageAccountName -ResourceGroupName $resourceGroup
#     $ctx = $storageAccount.Context
#     $listOfBLobs = Get-AzStorageBlob -Container $ContainerName -Context $ctx
    
#     foreach ($entry in $listOfBLobs) {
#         $name = $entry.name -split "/"
#         if ($name.Length -gt 1) {
#             $sub_folder = $name[0]
#             $entry.name = $name[1]
#         } else {
#             $sub_folder = $aplicacion
#             $entry.name = $name[0]
#         }
#         $entry | Add-Member -MemberType NoteProperty -Name sub_folder -value $sub_folder
#     }
#     $listOfBLobs = $listOfBLobs | Where-Object { $_.Name -like '*.zip' }
#     $listOfBlobs | ForEach-Object { $length = $_.Name.length; $_.Name = $_.Name.substring(0, $length - 4) }
#     $date = Get-Date -Format "dd/MM/yyyy HH:mm"
#     Write-Host "Download" $aplicacion $date
#     $name_file = -join ("C:\anexos_1\reprocesados_buenos\reclamos\", $aplicacion, ".txt");
#     $listOfBlobs | Select-Object sub_folder, Name, Length | Export-Csv -Delimiter '|' -NoTypeInformation $name_file
#     Write-Host "**************************************************"
# }
# Write-Host "Finalizamos"

# COMMAND ----------

# DBTITLE 1,Export de archivos generados en un blob Storage
# Export de archivos generados en un blob Storage

# Connect-AzAccount -Tenant "b5e244bd-c492-495b-8b10-61bfd453e423" -SubscriptionId "922a86de-0f41-4bb1-b1cb-4ab1fce2e718"
# $subscriptionId = "922a86de-0f41-4bb1-b1cb-4ab1fce2e718"
# $storageContainerDs = "rdh-auditoria"  #$containerNameDestino
# $localPath = "/home/juadaves/clouddrive/logReclamosBck2.txt"
# $storageContainer = "rdh-auditoria"  #$containerName

 
# $storageAccountRG = "historicos-produccion-rg-01"  #$resourceGroup
# $storageAccountName = "historicosprodsac01"  #$storageAccountName

# # Select right Azure Subscription
# Select-AzSubscription -SubscriptionId $SubscriptionId
 
# # Get Storage Account Key
# $storageAccountKey = (Get-AzStorageAccountKey -ResourceGroupName $storageAccountRG -AccountName $storageAccountName).Value[0]


# # Set AzStorageContext
# $destinationContext = New-AzStorageContext -StorageAccountName $storageAccountName -StorageAccountKey $storageAccountKey
 
# # Generate SAS URI
# $containerSASURI = New-AzStorageContainerSASToken -Context $destinationContext -ExpiryTime(get-date).AddSeconds(3600) -FullUri -Name $storageContainerDs -Permission rw
 
# # Upload File using AzCopy
# azcopy copy $localPath $containerSASURI

# COMMAND ----------

# DBTITLE 1,Conteo y tamaño para mas de 50.000 archivos (Evita MaxMemoryLenght)
# Conteo y tamaño para mas de 50.000 archivos (Evita sobre carga de memoria)

# # Desarrollo
# #Connect-AzAccount -Tenant "428f4e2e-13bf-4884-b364-02ef9af41a1d" -SubscriptionId "d5aae4c2-2452-4174-a138-fcec19d0bc75"

# # Produccion
# Connect-AzAccount -Tenant "b5e244bd-c492-495b-8b10-61bfd453e423" -SubscriptionId "922a86de-0f41-4bb1-b1cb-4ab1fce2e718"

# # Define Global Variables

# $subscriptionId = "922a86de-0f41-4bb1-b1cb-4ab1fce2e718"
# $storageContainerDs = "rdh-auditoria"  #$containerNameDestino
# $localPath = "/home/juan/"
# $storageContainer = "lotus-comex"  #$containerName

# # Define Variables create context
# $storageAccountRG = "historicos-produccion-rg-01"  #$resourceGroup
# $storageAccountName = "historicosprodsac01"  #$storageAccountName

# #1.get a reference to the storage account and the context
# $storageAccount = Get-AzStorageAccount -ResourceGroupName $storageAccountRG -Name $storageAccountName
# $ctx = $storageAccount.Context 

# # Define internal variables to count files
# $MaxReturn = 100000
# $ContainerName = "lotus-comex"
# $Total = 0
# $Token = $Null
# $auxAcumArray = @()
# $auxAcumArrayCount = 0
# $countIterationNewFile = 1
# $maxRowsCsv = 1000000

# #ciclo de lectura en bloques definidos en $MaxReturn
# do
# {
#     Get-Date -Format "dddd MM/dd/yyyy HH:mm K"

#     # Renovate context
#     $storageAccount = Get-AzStorageAccount -ResourceGroupName $storageAccountRG -Name $storageAccountName
#     $ctx = $storageAccount.Context 

#     # Obtener lista de archivos cada bloque definido en el MaxCount.
#     $Blobs = Get-AzStorageBlob -Container $ContainerName -Context $ctx -Blob "*.zip" -MaxCount $MaxReturn  -ContinuationToken $Token
#     $Total += $Blobs.Count
#     Write-Host "Acumulado: " $Total

#     #update elements in acum array 
#     $auxAcumArray += $Blobs
    
#     $auxAcumArrayCount = $auxAcumArray.Count
#     Write-Host "auxAcumArray: " $auxAcumArrayCount

   
#     $Token = $Blobs[$blobs.Count -1].ContinuationToken;
#     Write-Host "CountBlobs: " $blobs.Count
#     Get-Date -Format "dddd MM/dd/yyyy HH:mm K"
#     Write-Host "--------------------------------------------------------------------------"

#     #if increment each one millon of files, write new log File with the name and length of each record
#     if ($auxAcumArrayCount -ge $maxRowsCsv) {
    
#         # Renovate context
#         $storageAccount = Get-AzStorageAccount -ResourceGroupName $storageAccountRG -Name $storageAccountName
#         $ctx = $storageAccount.Context 

#         #Crear un nuevo archivo Csv Log y exportarlo en almacenamiento local /home/juan/logComexAud_1.txt ... 2..3 ..n
#         $fileCsvName = -join ("D:\anexos-comex\","logComexAud_", $countIterationNewFile, ".txt"); 
#         $auxAcumArray | Select-Object Name, Length | Export-Csv -Delimiter '|' -Path $fileCsvName
        
#         #Verifica archivo creado en la ruta
#         #ls -lf

#         #Reinicia array con acumulado
#         $auxAcumArray.clear()
#         $auxAcumArray = @()

#         #incrementa el contador para crear nuevo archivo lOG csv
#         $countIterationNewFile += 1    
        
#         Write-Host "Archivo CSV Exportado: " $fileCsvName " --- "
#         Get-Date -Format "dddd MM/dd/yyyy HH:mm K"

#         Write-Host $Token
#     }
# 	elseif ($auxAcumArrayCount -eq 125839){
# 		Write-Host "Ultimo archivo a generar.."

#         # Renovate context
#         $storageAccount = Get-AzStorageAccount -ResourceGroupName $storageAccountRG -Name $storageAccountName
#         $ctx = $storageAccount.Context

#         #Crear un nuevo archivo Csv Log y exportarlo en almacenamiento local /home/juan/logComexAud_1.txt ... 2..3 ..n
#         $fileCsvName = -join ("D:\anexos-comex\","logComexAud_", $countIterationNewFile, ".txt"); 
#         $auxAcumArray | Select-Object Name, Length | Export-Csv -Delimiter '|' -Path $fileCsvName
         
#         #Verifica archivo creado en la ruta
#         #ls -lf
 
#         #Reinicia array con acumulado
#         $auxAcumArray.clear()
#         $auxAcumArray = @()
 
#         #incrementa el contador para crear nuevo archivo lOG csv
#         $countIterationNewFile += 1    
         
#         Write-Host "Archivo CSV Exportado: " $fileCsvName " --- "
#         Get-Date -Format "dddd MM/dd/yyyy HH:mm K"
        
#         Write-Host "No hay mas Registros.. Finalizamos!!!"
# 	}
	
# 	 # if find read elements continue with the counting else break of the cicle.
#     if($Blobs.Length -le 0) { 
	
#         Break;
#     }
# }
# While ($Token -ne $Null)

# Write-Host "Total $Total blobs in container $ContainerName"
# Get-Date -Format "dddd MM/dd/yyyy HH:mm K"

# Disconnect-AzAccount

# COMMAND ----------

# DBTITLE 1,Sumatoria longitud files
# import os
# from pyspark.sql import functions as f

# path_files = "/mnt/rdh-auditoria-storage/"

# # list_result = [
# # 'logComexAud_1.txt',
# # 'logComexAud_2.txt',
# # 'logComexAud_3.txt',
# # 'logComexAud_4.txt',
# # 'logComexAud_5.txt',
# # 'logComexAud_6.txt',
# # 'logComexAud_7.txt',
# # 'logComexAud_8.txt',
# # 'logComexAud_9.txt',
# # 'logComexAud_10.txt',
# # 'logComexAud_11.txt']

# list_result = [x for x in os.listdir('/dbfs' + path_files) if 'logComex' in x]

# total = 0

# for file in list_result:
#   df_file_name = spark.read.csv(path_files + file, header = True, sep = '|')
#   peso_file = df_file_name.select(f.sum('Length').alias('total')).first()['total']
#   print("archivo:", file, "peso:", peso_file)
#   total = total + peso_file

# print("peso total:", total)
