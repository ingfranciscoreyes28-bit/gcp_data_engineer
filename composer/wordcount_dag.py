from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from datetime import datetime

# Definir DAG
default_args = {
    'owner': 'data-engineer',
    'start_date': datetime(2025, 1, 1),
    'retries': 1
}

dag = DAG(
    'wordcount_dataflow',
    default_args=default_args,
    description='Ejecutar job de Dataflow desde template',
    schedule_interval=None,
    catchup=False,
)

dataflow_task = DataflowTemplatedJobStartOperator(
    task_id='ejecutar_wordcount',
    template='gs://gcp-bucket-777/templates/wordcount_template',
    location='us-central1',
    project_id='gcp-data-engineer-09',
    dag=dag,
)