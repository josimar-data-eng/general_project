import json
import pandas as pd
from google.cloud import bigquery

from google.oauth2 import service_account #protocol to client-server communication to get authorization
key_path = "/Users/josimardossantosjunior/Code/DataEngineeringOnGCP/cloud_function/credential.json"
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials= credentials,project=credentials.project_id)

### Converts schema dictionary to BigQuery's expected format for job_config.schema
def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

### Create dummy data to load
df = pd.DataFrame([[2, 'Jane', 'Doe']]
                  ,columns=['id', 'first_name', 'last_name']
                  )
print(type(df))

### Convert dataframe to JSON object
# json_data = df.to_json(orient = 'records')
# print(type(json_data))
# json_object = json.loads(json_data)

# print(json_object)
# print(type(json_object))


# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"<My_Credentials_Path>\application_default_credentials.json"

### Define schema as on BigQuery table, i.e. the fields id, first_name and last_name   
# table_schema = {
#           'name': 'id',
#           'type': 'INTEGER',
#           'mode': 'REQUIRED'
#           }, {
#           'name': 'first_name',
#           'type': 'STRING',
#           'mode': 'NULLABLE'
#           }, {
#           'name': 'last_name',
#           'type': 'STRING',
#           'mode': 'NULLABLE'
#           }

# project_id=client.project
# dataset_id = "fligh_etl_job"
# table_id = "qas-project-363822.fligh_etl_job.test"



# job_config = bigquery.LoadJobConfig()
# job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
# job_config.schema = format_schema(table_schema)
# job = client.load_table_from_json(json_object, table_id, job_config = job_config)

# print(job.result())