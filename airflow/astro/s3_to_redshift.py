# Requirements:
# 1 - Install the apache-airflow-providers-amazon package on Airflow Environment 6.2.0 version.
# 2 - Create a aws connection(aws_conn_id on code) in airflow environment using the AWS-Access-Key-ID, and AWS-Secret-Access-Key generetade from AWS account (there were already a bucket)
# google_cloud_default is the deafult id connection from gcp it doesn't nedd to do anything about it.

#### IT'S NOT WORKINK, ONLY A DRAFT #####

from airflow import models, DAG
from datetime import date, datetime

from airflow.models import *
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator

current_date = str(date.today())
project_id="dev-project-363923"

default_dag_args = {
     "owner":"josimar-junior"
    ,"depends_on_past": True
    ,"start_date": datetime.utcnow()
    ,"email_on_failure": False
    ,"email_on_retry": False
    ,"project_id": project_id
    ,"scheduled_interval": None
}

with DAG("move-from-s3-to-gcs", default_args=default_dag_args) as dag:

    s3_to_gcs_op = S3ToGCSOperator(
        task_id="s3_to_gcs_example",
        bucket="source-s3-bucket",
        prefix="avg",
        gcp_conn_id="google_cloud_default",
        aws_conn_id="aws_default",
        dest_gcs="gs://target-gcs-bucket",
        replace=False,
        gzip=False
    )

    s3_to_gcs_op