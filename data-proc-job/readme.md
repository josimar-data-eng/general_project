

## Pipeline Description

### It's a pipeline that uses dataproc, storage and bigquery, where the dataproc cluster run a pyspark job, that get json files from storage, parse it do dataframe, load in an output storage and use it as input to load function that load in bigquery throgh a fo loop to get all blobs from a specific bukcet, using APPEND in the write_disposition.


## Project Structure
### └── main.py
### └── flights_etl.py

### flights_etl module: Contain methods 
#### schema: Just return the list schema to send it to load function that will use schema in the job_configo
#### transform: Get data from input storage, run queries, save in df
#### load_flight_nums_data_in_storage: Get the df from tranform method , and load a json file in storage (output)
#### load_avg_delays_by_flight_nums: Get the file from json output storage


### Resources: Cloud SDK, Python, Dataproc, Storage, BigQuery


# Submit PySpark Job
gcloud dataproc jobs submit pyspark \
--cluster=flight-etl-job \
--py-files {bucket}/flights_etl.py gs://{bucket}/main.py
