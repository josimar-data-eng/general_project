from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account #protocol to client-server communication to get authorization

#Authentication
key_path = "/Users/josimardossantosjunior/Code/DataEngineeringOnGCP/cloud_function/credential.json"
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials= credentials,project=credentials.project_id)

def create_dataset(dataset_name):
    try:
        client.get_dataset(dataset_name)  # Make an API request.
        print("Dataset {} already exists".format(dataset_name))
        return client.get_dataset(dataset_name)
    except NotFound:
        print("Dataset {} is not found".format(dataset_name))
        dataset_id = "{}.{}".format(client.project,dataset_name)
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "us-central1"
        dataset = client.create_dataset(dataset, exists_ok=True, timeout=30)  # Make an API request.
        # dataset_string = print("{}.{}".format(client.project, dataset.dataset_id))
        return dataset
