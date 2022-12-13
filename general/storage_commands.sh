gsutil mb -c standard -l us-central1 -p dev-project-363923 gs://flights-dev-data-in &&\
gsutil mb -c standard -l us-central1 -p dev-project-363923 gs://flights-dev-data-out &&\
gsutil mb -c standard -l us-central1 -p dev-project-363923 gs://flights-dev-data-proc-job 

cd /Users/josimardossantosjunior/Code/general_project/flight_elt_job/flights-data
gsutil cp *.json gs://flights-data-in

gsutil cp main.py flights_etl.py gs://flights-data-job


gsutil cp \
/Users/josimardossantosjunior/Code/DataEngineeringOnGCP/cloud_function/avg_delays_by_flight_nums.json \
gs://flights-data-out


gsutil cp \
/Users/josimardossantosjunior/Downloads/avg_delays_by_flight_nums-0.json \
gs://flights-data-out


#Removes all versions of all objects in a bucket, and then deletes the bucket:
gsutil rm -r gs://bucket

#Remove all objects and their versions from a bucket without deleting the bucket, use the -a option:
gsutil rm -a gs://bucket/**