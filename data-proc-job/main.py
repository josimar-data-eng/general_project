from datetime import datetime
from google.cloud import storage
from flights_etl import schema, transform, load_flight_nums_data_in_storage, load_avg_delays_by_flight_nums

storage_client = storage.Client()

current_datetime            = datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")
output_flight_nums_bucket   = "gs://flights-dev-data-out/avg_delays_by_flight_nums"
flight_nums_table_id        = "dev-project-363923.flight_data.avg_delay_flight_nums"
flight_nums_uri             = output_flight_nums_bucket+"/avg_delays_by_flight_nums_"+current_datetime


if __name__ == '__main__':
    load_flight_nums_data_in_storage(transform(), flight_nums_uri)    
    blobs = storage_client.bucket("flights-dev-data-out").list_blobs()
    for blob in blobs:
        if "avg_delays_by_flight_nums" in blob.name and blob.name.endswith('.json'):
            load_avg_delays_by_flight_nums((flight_nums_uri+"/*"),flight_nums_table_id, schema())