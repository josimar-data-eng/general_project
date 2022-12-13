#create clustes
gcloud dataproc clusters create flight-etl-job \
--region "us-central1" --zone "us-central1-c" \
--master-machine-type n1-standard-4 --master-boot-disk-size 500 --num-workers 2 \
--worker-machine-type n1-standard-4 --worker-boot-disk-size 500 \
--image-version 1.5-debian10 \
--optional-components ANACONDA,JUPYTER \
--project dev-project-363923 \
--enable-component-gateway

#SUBMITTING JOB
#from local script
gcloud dataproc jobs submit pyspark --cluster=flight-etl-job \
/Users/josimardossantosjunior/Code/DataEngineeringOnGCP/flights_etl.py \
--custom-flag

#from bucket
gcloud dataproc jobs submit pyspark \
--cluster=flight-etl-job \
--py-files gs://flights-data-proc-job/flights_etl.py gs://flights-data-proc-job/main.py

#stop cluster
gcloud dataproc clusters stop flight-etl --region="us-central1"

#restart cluster
gcloud dataproc clusters start flight-etl --region="us-central1"

#delete cluster
gcloud dataproc clusters delete flight-etl --region "us-central1"
