import csv
import sys
import pprint
from google.cloud import bigquery
pp = pprint.PrettyPrinter(indent=4)
import os
import pandas as pd
import numpy as np

# constants
SERVICE_KEY_jSON = './service_key.json'

# set OS environment
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=SERVICE_KEY_jSON

def getBoroghFromLatLong(lat, long):

    returnVal = ""
    client = bigquery.Client()         # Start the BigQuery Client
    QUERY = ('SELECT UPPER(borough) AS BOROUGH FROM `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` tz_loc WHERE (ST_DWithin(tz_loc.zone_geom, ST_GeogPoint('+ str(long) +', '+ str(lat) +'),0))')
    
    query_job = client.query(QUERY)    # Start Query API Request
    query_result = query_job.result()  # Get Query Result 
    df = query_result.to_dataframe() 

    try:
        returnVal = str(df["BOROUGH"].values[0])
    except IndexError:
        returnVal = "Unknown"

    return returnVal
    

res1 = getBoroghFromLatLong(40.680088,-73.94398)
res2 = getBoroghFromLatLong(40.8063462,-73.9331715)
#res = getBoroghFromLatLong(40.7576691,-73.9591454)
print(res1)
print(res2)
#print(type(res))



# '40.8063462'
# '-73.9331715'

# '40.7576691'
# '-73.9591454'
