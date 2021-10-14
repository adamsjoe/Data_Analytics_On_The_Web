import os
import modin.pandas as pd
from google.cloud import bigquery
from datetime import datetime
import pprint


# constants
SERVICE_KEY_jSON = './service_key.json'

# functions
def gimmeADate(dfValIn):

    tmp_date = dfValIn
    # date_element = tmp_date.date()    
    return str(tmp_date)

def getBoroghFromLatLong(lat, long):
    client = bigquery.Client()         # Start the BigQuery Client
    QUERY = ('SELECT UPPER(borough) FROM `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` tz_loc WHERE (ST_DWithin(tz_loc.zone_geom, ST_GeogPoint('+ str(long) +', '+ str(lat) +'),0))')
    
    query_job = client.query(QUERY)    # Start Query API Request
    query_result = query_job.result()  # Get Query Result 
    df = query_result.to_dataframe() 

    return df.to_string(header=False, index=False)

# set OS environment
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=SERVICE_KEY_jSON

client = bigquery.Client()         # Start the BigQuery Client
#QUERY = ('SELECT * FROM `uhi-assignment-1.assignment.collated_data`')
QUERY = ('select * from `uhi-assignment-1.assignment.collated_data`where collision_date = "2021-10-05" order by collision_date LIMIT 10')

query_job = client.query(QUERY)    # Start Query API Request
query_result = query_job.result()  # Get Query Result
df = query_result.to_dataframe() 

result_dictionary = {}
borough_dictionary = {}
row = 0
