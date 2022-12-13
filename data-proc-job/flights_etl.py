#!/usr/bin/env python
# coding: utf-8
import os
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext
from google.cloud import bigquery,storage

bq_client = bigquery.Client()
storage_client = storage.Client()

sc = SparkContext.getOrCreate()
spark = SQLContext(sc)
current_datetime = datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")


input_bucket_name = "gs://flights-dev-data-in"
output_flight_nums_bucket = "gs://flights-dev-data-out/avg_delays_by_flight_nums"
output_distance_category_bucket = "gs://flights-dev-data-out/avg_delays_by_distance_category"
current_datetime = datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")

flight_nums_table_id = "dev-project-363923.flight_data.avg_delay_flight_nums"
flight_nums_uri      = output_flight_nums_bucket+"/avg_delays_by_flight_nums_"+current_datetime

distance_category_table_id = "avg_delays_by_distance_category"
distance_category_uri      = output_distance_category_bucket+"/avg_delays_by_distance_category_"+current_datetime


def transform():
    # Turn flight_data into a data-frame and Read all files inside fligh-data-in directory and create a dataframe
    flights_data = spark.read.json(input_bucket_name)
    print("flights_data df done")

    #create a temporary table from dataframe and load the result in a new dataframe
    flights_data.registerTempTable("flights_data_temp_table")
    avg_by_flight_query = """
            select 
                flight_date , 
                round(avg(arrival_delay),2) as avg_arrival_delay,
                round(avg(departure_delay),2) as avg_departure_delay,
                flight_num 
            from 
                flights_data_temp_table 
            group by 
                flight_num , 
                flight_date 
        """
    avg_delays_by_flight_nums = spark.sql(avg_by_flight_query)
    print("avg_delays_by_flight_nums done")


    add_distance_category_flights_query = """
            select 
                *,
                case 
                    when distance between    0 and  500 then 1 
                    when distance between  501 and 1000 then 2
                    when distance between 1001 and 2000 then 3
                    when distance between 2001 and 3000 then 4 
                    when distance between 3001 and 4000 then 5 
                    when distance between 4001 and 5000 then 6 
                end distance_category 
            from 
                flights_data_temp_table 
            """
    flights_data = spark.sql(add_distance_category_flights_query)
    print("add_distance_category_flights_query done")


    flights_data.registerTempTable("flights_data_temp_table")
    avg_by_distance_query = """
            select 
                flight_date , 
                round(avg(arrival_delay),2) as avg_arrival_delay,
                round(avg(departure_delay),2) as avg_departure_delay,
                distance_category 
            from 
                flights_data_temp_table 
            group by 
                distance_category , 
                flight_date 
        """
    avg_delays_by_distance_category = spark.sql(avg_by_distance_query)
    print("avg_delays_by_distance_category done\n")

    print("transformation finished")
    
    return avg_delays_by_flight_nums


def load_flight_nums_data_in_storage(df, uri):
    #Write Spark DataFrame to JSON file
    df.coalesce(1).write.mode('ignore').format("json").save(uri)
    print("load_flight_nums_data_in_storage done\n")


def schema():
    flight_nums_schema =[
                    ('avg_departure_delay' , 'FLOAT'   , 'REQUIRED')
                    ,('avg_arrival_delay'   , 'FLOAT'   , 'REQUIRED')
                    ,('flight_num'          , 'INTEGER' , 'REQUIRED')
                    ,('flight_date'         , 'DATE'    , 'REQUIRED')
                    ]
    return flight_nums_schema
    
def load_avg_delays_by_flight_nums(uri,table_id,schema):
    print("got load_json_to_bq")

    job_config = bigquery.LoadJobConfig(
         schema=[bigquery.SchemaField(*schema) for schema in schema]
        ,source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        ,write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    )
    print("got job_config")

    load_job = bq_client.load_table_from_uri(
         uri
        ,table_id
        ,location="us-central1"
        ,job_config=job_config
    )
    print("got load_job")    

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows)) 


# # def load_distance_category_data_in_storage(df, uri):
#     #Write Spark DataFrame to JSON file
#     avg_delays_by_distance_category.coalesce(1).write.mode('ignore').format("json").save(distance_category_uri)
#     print("ok")    
