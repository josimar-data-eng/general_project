gcloud composer environments create airflow-jobs \
--location=us-central1 \
--service-account=general-projects-675@dev-project-363923.iam.gserviceaccount.com

gcloud composer environments update airflow-jobs \
    --location us-central1 \
     --update-pypi-package apache-airflow-providers-amazon==6.2.0
