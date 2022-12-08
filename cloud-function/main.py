import os, json
from google.cloud import bigquery, storage
from table_module import format_schema
from load_module import load_json_to_bq
# import dataset_module as dataset


#Authentication
# from google.oauth2 import service_account #protocol to client-server communication to get authorization
# key_path = "/Users/josimardossantosjunior/Code/DataEngineeringOnGCP/cloud_function/credential.json"
# credentials = service_account.Credentials.from_service_account_file(key_path)
# client = bigquery.Client(credentials= credentials,project=credentials.project_id)

bq_client = bigquery.Client()
storage_client = storage.Client()

# project=bq_client.project
dataset_id = "fligh_etl_job"
table_id = "qas-project-363822.fligh_etl_job.avg_delay_flight_nums"
schema_path = os.getcwd()+"/bigquery_schema/avg_delay_flight_nums.json"


def load(data,context):
    print(f"Content: {data}")
    print(f"Event:   {context}")

    bucket = storage_client.bucket(data["bucket"])
    blob = bucket.get_blob(data["name"])
    # uri = "gs://{}/*".format(data["bucket"])
    # load_json_to_bq(uri, table_id, format_schema(schema_path))
    load_json_to_bq(blob,table_id, format_schema(schema_path))    

    # file_uri = 'gs://%s/%s' % (data['bucket'], data['name'])
    # dataset.create_dataset(dataset_id)    
    # table.create_table(table_id,schema_path)
