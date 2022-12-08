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

def load_json_to_bq(blob, table_id,schema):
# def load_json_to_bq(uri,table_id,schema):

    json_data_string = blob.download_as_string()
    json_data = ndjson.loads()
    # text_data = blob.download_as_text()

    json_list = []
    with open("avg.json") as file:
        for each_row in file:
            dict_file = json.loads(each_row)
            json_list.append(dict_file)

    avg_df = pd.DataFrame(json_list)
    avg_df["load_datetime"] = datetime.now()

    print(avg_df)

    print("got load_json_to_bq")
    job_config = bigquery.LoadJobConfig(
         schema=schema
        ,source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        ,write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    #1st approach when the data loaded was the file directly
    # print("got job_config")
    # load_job = bq_client.load_table_from_uri(
    #      uri
    #     ,table_id
    #     ,location="us-central1"
    #     ,job_config=job_config
    # )  # Make an API request.


    load_job = bq_client.load_table_from_json(
          json_object
        , table_id
        , location="us-central1"
        , job_config = job_config
    )


    print("got load_job")    

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))    