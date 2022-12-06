import json
import pandas as pd
from google.cloud import bigquery

#Authentication
# from google.oauth2 import service_account #protocol to client-server communication to get authorization
# key_path = "/Users/josimardossantosjunior/Code/DataEngineeringOnGCP/cloud_function/credential.json"
# credentials = service_account.Credentials.from_service_account_file(key_path)
# client = bigquery.Client(credentials= credentials,project=credentials.project_id)

client = bigquery.Client()

def load_json_to_bq(uri,table_id,schema):

    print("got load_json_to_bq")

    job_config = bigquery.LoadJobConfig(
         schema=schema
        ,source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        ,write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    print("got job_config")    

    load_job = client.load_table_from_uri(
         uri
        ,table_id
        ,location="us-central1"
        ,job_config=job_config
    )  # Make an API request.

    print("got load_job")    

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))    