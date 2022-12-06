import os, json
import create_table as table
import load_json_to_bq as load
import create_dataset as dataset
from google.cloud import bigquery

#Authentication
from google.oauth2 import service_account #protocol to client-server communication to get authorization
key_path = "/Users/josimardossantosjunior/Code/DataEngineeringOnGCP/cloud_function/credential.json"
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials= credentials,project=credentials.project_id)

project=client.project
dataset_id = "fligh_etl_job"
table_id = "qas-project-363822.fligh_etl_job.avg_delay_flight_nums_2"
schema_path = os.getcwd()+"/bigquery_schema/avg_delay_flight_nums.json"



uri = "/Users/josimardossantosjunior/Code/DataEngineeringOnGCP/cloud_function/avg_delays_by_flight_nums.json"


schema = json.load(open(uri))
# json_object = json.dumps(schema)

# print((list(schema)))


# json_object = json.loads(str(schema))

# print(json_object)
# print(type(json_object))


# dataset.create_dataset(dataset_id)
# table.create_table(table_id,schema_path)
load.load_json_to_bq(uri, table_id, table.format_schema(schema_path))
