from datetime import date, datetime, timedelta
from airflow import models, DAG

from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.models import *
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators import BashOperator





current_date = str(date.today())
project_id="dev-project-363923"
pyspark_job = "gs://flights-dev-data-proc-job/main.py"
pyfiles = ["gs://flights-dev-data-proc-job/flights_etl.py", "gs://flights-dev-data-proc-job/main.py" ]
cluster_name = "ephemeral-spark-cluster-{{ds_nodash}}"  # https://airflow.apache.org/docs/apache-airflow/1.10.5/macros.html

default_dag_args = {
     "owner":"josimar-junior"
    ,"depends_on_past": True
    ,"start_date": datetime.utcnow()
    ,"email_on_failure": False
    ,"email_on_retry": False
    ,"retries": 1
    ,"retry_delay": timedelta(minutes=5)
    ,"project_id": project_id
    ,"scheduled_interval": None
}

with DAG("flighs_etl", default_args=default_dag_args) as dag:

    create_cluster = DataprocClusterCreateOperator(
         task_id="create_dataproc_cluster"
        ,cluster_name=cluster_name
        ,master_machine_type="n1-standard-4"
        ,worker_machine_type="n1-standard-4"
        ,num_workers=2
        ,region="us-central1"
        ,zone ="us-central1-c"
        ,master_disk_size="500"
        ,worker_disk_size="500"
    )

    submit_pyspark_job = DataprocSubmitPySparkJobOperator(
         task_id = "submit_flight_etl_pyspark_job"
        ,main = pyspark_job
        ,pyfiles=pyfiles
        ,cluster_name=cluster_name
        ,region="us-central1"        
    )

    delete_cluster = BashOperator(
         task_id = "delete_dataproc_cluster"
        ,bash_command = f"gcloud dataproc cluster delete {cluster_name} --region=us-central1"
    )

    create_cluster >> submit_pyspark_job >> delete_cluster