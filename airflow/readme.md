##The PySpark job is returning erro. See it after.


Composer is the managed version of Apache Airflow

1 - Create a composer trough CLI

        gcloud composer environments create airflow-jobs \
        --location=us-central1 \
        --service-account=general-projects-675@dev-project-363923.iam.gserviceaccount.com

        I got error trying to setting all these following flags:
        --zone=us-central1-c \
        --machine-type=n1-standard-2 \
        --node-count=3 \
        --disk-size=30 \
        --python-version=3 \
        --airflow-version=1[.10[.15]] \                  
        --image-version=composer-1[.20[.1]]-airflow-2[.3[.4]] \
        <!-- --scheduler-count=1 -> Only available for airflow v2--> 
        <!-- --env-variables=[NAME=VALUE,…] -->


2 - how use environment variables instead of exposure them?

3 - default_dag_args is a python dict.
 - owner
 - dependes_on_past
 - start_date
 - email_on_failure
 - email_on_retry
 - retries
 - retry_dalay
 - project_id
 - schedule_interval

4 - with dag() as dag:
    
    task_name = OPERATOR(
        task_id = 
        ...
        ...    
    )

