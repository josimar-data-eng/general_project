#create clustes
gcloud dataproc clusters create flight-etl \
--region "us-central1" --zone "us-central1-c" \
--master-machine-type n1-standard-4 --master-boot-disk-size 500 --num-workers 2 \
--worker-machine-type n1-standard-4 --worker-boot-disk-size 500 \
--image-version 1.5-debian10 \
--optional-components ANACONDA,JUPYTER \
--project qas-project-363822 \
--enable-component-gateway

#SUBMITTING JOB
#from local script
gcloud dataproc jobs submit pyspark --cluster=flight-etl \
/Users/josimardossantosjunior/Code/DataEngineeringOnGCP/flights_etl.py \
--custom-flag

#from bucket
gcloud dataproc jobs submit pyspark \
--cluster=flight-etl \
--py-files gs://flights-data-job/flights_etl.py gs://flights-data-job/main.py

#stop cluster
gcloud dataproc clusters stop flight-etl --region="us-central1"

#restar cluster
gcloud dataproc clusters start flight-etl --region="us-central1"

#delete cluster
gcloud dataproc clusters delete flight-etl --region "us-central1"
