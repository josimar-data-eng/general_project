#Authentication trhough SDK
gcloud init

#crete input, output and bucket to store the etl files
gsutil mb -c standard -l us-central1 -p dev-project-363923 gs://flights-dev-data-in &&\
gsutil mb -c standard -l us-central1 -p dev-project-363923 gs://flights-dev-data-out &&\
gsutil mb -c standard -l us-central1 -p dev-project-363923 gs://flights-dev-data-proc-job 

#send the input json file to input bucket
cd pwd/data-proc-job/flights-data
gsutil cp *.json gs://flights-data-in


#create dataset
bq --locaion=us-central1 mk -d \
    --description="Flight data dataset" \
    dev-project-363923:flight_data


#create clustes
gcloud dataproc clusters create flight-etl-job \
--region "us-central1" --zone "us-central1-c" \
--master-machine-type n1-standard-4 --master-boot-disk-size 500 --num-workers 2 \
--worker-machine-type n1-standard-4 --worker-boot-disk-size 500 \
--image-version 1.5-debian10 \
--optional-components ANACONDA,JUPYTER \
--project dev-project-363923 \
--enable-component-gateway


# Send pyfiles to flight-data-proc-job bucket
gsutil cp /Users/josimardossantosjunior/Code/general_project/data-proc-job/flights_etl.py gs://flights-dev-data-proc-job &&\
gsutil cp /Users/josimardossantosjunior/Code/general_project/data-proc-job/schema.py gs://flights-dev-data-proc-job &&\
gsutil cp /Users/josimardossantosjunior/Code/general_project/data-proc-job/main.py gs://flights-dev-data-proc-job


# Submit PySpark Job
gcloud dataproc jobs submit pyspark \
--cluster=flight-etl-job \
--py-files gs://flights-dev-data-proc-job/flights_etl.py gs://flights-dev-data-proc-job/main.py 