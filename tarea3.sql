# Pseudocódigo sencillo para cargar users_cleaned.parquet a BigQuery

1. CONFIGURAR CREDENCIALES
   • Definir ruta al archivo de credenciales JSON de la cuenta de servicio:
     GOOGLE_APPLICATION_CREDENTIALS = "/00034/a/bigquery-service-account.json"
   • (En el entorno) Exportar variable de entorno:
     export GOOGLE_APPLICATION_CREDENTIALS="/00034/a/bigquery-service-account.json"

2. INICIALIZAR CLIENTE DE BIGQUERY
   • CLIENTE ← bigquery.Client(PROJECT_ID = "Copia de Copia de prueba_espaniol_de")

3. VERIFICAR O CREAR DATASET
   • DATASET_REF ← CLIENTE.dataset("analytics_dataset")
   • Intentar:
       CLIENTE.get_dataset(DATASET_REF)
       → Si existe, imprimir "Dataset existe"
     Capturar NotFound:
       DATASET_OBJ ← bigquery.Dataset(DATASET_REF)
       DATASET_OBJ.location ← "US"          
       CLIENTE.create_dataset(DATASET_OBJ)
       → Imprimir "Dataset creado"

4. PREPARAR REFERENCIA A LA TABLA
   • TABLE_REF ← DATASET_REF.table("users_cleaned")

5. CONFIGURAR JOB DE CARGA
   • JOB_CONFIG ← new LoadJobConfig()
   • JOB_CONFIG.source_format ← PARQUET
   • JOB_CONFIG.write_disposition ← WRITE_TRUNCATE
   • (Opcional) Definir esquema manual en JOB_CONFIG.schema

6. EJECUTAR JOB DE CARGA
   Si PARQUET_URI comienza con "gs://":
     • LOAD_JOB ← CLIENTE.load_table_from_uri(
         source_uri = PARQUET_URI,
         destination = TABLE_REF,
         job_config = JOB_CONFIG
       )
   Sino:
     • Abrir archivo local en modo binario:
         ARCHIVO ← abrir(PARQUET_URI, "rb")
       LOAD_JOB ← CLIENTE.load_table_from_file(
         file_obj = ARCHIVO,
         destination = TABLE_REF,
         job_config = JOB_CONFIG
       )
   • Imprimir "Carga iniciada. ID del job: " + LOAD_JOB.job_id
   • Esperar a que LOAD_JOB termine (por ejemplo, LOAD_JOB.result(timeout=600))
     Capturar excepciones:
       → Imprimir "Error en carga: " + mensaje de error
       → Terminar proceso

7. VERIFICAR RESULTADO (OPCIONAL)
   • TABLA ← CLIENTE.get_table(TABLE_REF)
   • Imprimir "Número de registros en tablas: " + TABLA.num_rows

8. FINALIZAR
   • Imprimir "Proceso de carga completado con éxito"
