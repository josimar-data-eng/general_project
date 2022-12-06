#!/usr/bin/env python
# coding: utf-8

import os
from datetime import datetime
from pyspark import SparkContext
from google.cloud import bigquery
from pyspark.sql import SQLContext
from schema_module import format_schema
client = bigquery.Client()

sc = SparkContext.getOrCreate()
spark = SQLContext(sc)
current_datetime = datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")

# distance_category_table_id = "avg_delays_by_distance_category"
# distance_category_uri = output_bucket_name+"/avg_delays_by_distance_category"+current_datetime

input_bucket_name = "gs://flights-data-in"
output_bucket_name = "gs://flights-data-out"
current_datetime = datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")

flight_nums_table_id = "qas-project-363822.fligh_etl_job.avg_delay_flight_nums"
flight_nums_uri = output_bucket_name+"/avg_delays_by_flight_nums_"+current_datetime

print("ok")




# def transform():



# Turn flight_data into a data-frame and Read all files inside flightd_data directory and create a dataframe
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
print("avg_by_flight_query done")

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
print("avg_by_distance_query done\n")

print("transformation finished")

#Write Spark DataFrame to JSON file

avg_delays_by_flight_nums.coalesce(1).write.mode('ignore').format("json").save(flight_nums_uri)
print("ok")

# avg_delays_by_distance_category.coalesce(1).write.mode('ignore').format("json").save(flight_nums_uri)
# print("ok")


def load_avg_delays_by_flight_nums(uri,table_id,schema):

    print("got load_json_to_bq")

    job_config = bigquery.LoadJobConfig(
         schema=[bigquery.SchemaField(*schema) for schema in schema]
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

print("ok")    

schema =[
         ('avg_departure_delay', 'FLOAT', 'REQUIRED')
        ,('avg_arrival_delay', 'FLOAT', 'REQUIRED')
        ,('flight_num', 'INTEGER', 'REQUIRED')
        ,('flight_date', 'DATE', 'REQUIRED')
        ]

load_avg_delays_by_flight_nums((flight_nums_uri+"/*"),flight_nums_table_id, schema)

if __name__ == '__main__':
    transform()   