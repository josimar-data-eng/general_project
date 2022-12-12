import json
import ndjson
import pandas as pd
from datetime import datetime
from google.cloud import bigquery, storage

#Authentication
# from google.oauth2 import service_account #protocol to client-server communication to get authorization
# key_path = "/Users/josimardossantosjunior/Code/DataEngineeringOnGCP/cloud_function/credential.json"
# credentials = service_account.Credentials.from_service_account_file(key_path)
# client = bigquery.Client(credentials= credentials,project=credentials.project_id)

bq_client = bigquery.Client()
storage_client = storage.Client()

#2nd approach
def load_json_to_bq(blob, table_id,schema):
# def load_json_to_bq(uri,table_id,schema):

    print("start load_json_to_bq")
    json_data_string = blob.download_as_string()
    json_data = ndjson.loads(json_data_string)
    # text_data = blob.download_as_text()


    print("start loop json_list")
    json_list = []
    for row in json_data:
        json_list.append(row)

    print("start dataframe")
    avg_df = pd.DataFrame(json_list)
    avg_df["load_datetime"] = datetime.now()
    print(avg_df)


    print("start job_config")
    job_config = bigquery.LoadJobConfig(
         schema=schema
        ,source_format=bigquery.SourceFormat.CSV
        ,write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    print("finish job_config")    

    load_job = bq_client.load_table_from_dataframe(
        avg_df, table_id, job_config=job_config
    )  # Make an API request.
    print("got load_job")    

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows)) 