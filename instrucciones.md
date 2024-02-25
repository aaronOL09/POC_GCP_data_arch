# 1 Crear en GCP:
#     - proyecto
#     - bucket en GCS
#     - BigQuery: dataset aotelop_edwh_raw
# 2 Subir archivos de logs/txt de test a bucket en directorio que se desee
# 3 Habilitar APIs de los servicios (Dataflow, Compute Engine, Cloud Logging, Cloud Storage, Google Cloud Storage JSON, BigQuery, Cloud Pub/Sub, Cloud Datastore, and Cloud Resource Manager.) y permisos de administrador. Ver https://cloud.google.com/pubsub/docs/stream-messages-dataflow?hl=es-419#python .

# Ejecución desde local (Windows):
# Prerrequisitos: tener instalado python y pip (probado con 3.10.11 y 23.0.1 respectivamente)
# Crear y activar venv
pip install virtualenv
python -m venv myenv
.\env\Scripts\Activate.ps1
# Instalar dependencias
pip install -r .\requirements.txt
# Instalar gcloud (https://cloud.google.com/sdk/docs/install?hl=es-419)
# Conectar a GPC
gcloud auth application-default login
# Ejecución de pipeline
python .\main_pipeline.py --input gs://bucket/path/chosen/input/*.txt --output gs://bucket/path/chosen/ouput/ --bqproject your_bq_project
#   Donde:
#     --input es el directorio del punto 2 con el patrón *.txt en el bucket creado
#     --output es el directorio en el que se generarán los json y a los que apuntará la tabla externa
#     --bqproject es el projecto bigquery creado






# EXTRA. EJECUCIÓN POR PARTES

# Dataflow
# Ejecutar (substituyendo bucket por el nombre del bucket creado)
python dataflow/beam_txt_to_json_by_file.py --input gs://bucket/txt/*.txt --output gs://bucket/json/

# dbt
# Modificar nombre de projecto BigQuery en fichero dbt/dbt_challenge/profiles.yml (linea 12, project) o crear la variable de entorno
# Ejecutar dbt
cd dbt
dbt run
#### UNA VEZ FINALIZADO, SE VERÁ EL RESULTADO DE LA MEDIANA EN LA TABLA 


