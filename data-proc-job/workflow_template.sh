
#It's failing. Go back here after to know what's going on. 

template_name="flights_etl"
cluster_name="spark-job-flights"

gcloud dataproc workflow-templates delete -q $template_name  &&

# gsutil cp /Users/josimardossantosjunior/Code/general_project/data-proc-job/flights_etl_workflow.py gs://flights-dev-data-proc-job &&

gcloud dataproc workflow-templates create $template_name &&

gcloud dataproc workflow-templates set-managed-cluster $template_name --zone "us-central1-c" \
--cluster-name=$cluster_name \
--region=us-central1 \
--scopes=default \
--master-machine-type n1-standard-2 \
--master-boot-disk-size 20 \
--num-workers 2 \
--worker-machine-type n1-standard-2 \
--worker-boot-disk-size 20 \
--image-version 1.3 &&


gcloud dataproc workflow-templates \
add-job pyspark PY_FILE=gs://flights-dev-data-proc-job/flights_etl_workflow.py \
--region=us-central1 \
--step-id=flight_delays_etl \
--workflow-template=$template_name &&
# Check after why $template_name is not working here
# [--start-after=STEP_ID,[STEP_ID,…]] 


gcloud dataproc workflow-templates instantiate \
flights_etl \
--region=us-central1
# Check after why $template_name is not working here


