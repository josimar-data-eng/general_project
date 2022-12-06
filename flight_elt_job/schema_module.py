import json
from google.cloud import bigquery
# from google.oauth2 import service_account #protocol to client-server communication to get authorization

#Authentication
# key_path = "/Users/josimardossantosjunior/Code/DataEngineeringOnGCP/cloud_function/credential.json"
# credentials = service_account.Credentials.from_service_account_file(key_path)
# client = bigquery.Client(credentials= credentials,project=credentials.project_id)

client = bigquery.Client()

### Converts schema dictionary to BigQuery's expected format for job_config.schema
def format_schema(path):
    schema = json.load(open(path))
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

 