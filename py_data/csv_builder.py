import os
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

# constants
SERVICE_KEY_jSON = 'service_key.json'

def gimmeADate(dfValIn):

    tmp_date = dfValIn#.values[0][7]
    #datetime_element = tmp_date.values[0][7]
    date_element = tmp_date.date()    
    return date_element

def getBoroghFromLatLong(lat, long):
    client = bigquery.Client()         # Start the BigQuery Client
    QUERY = ('SELECT UPPER(borough) FROM `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` tz_loc WHERE (ST_DWithin(tz_loc.zone_geom, ST_GeogPoint('+ str(long) +', '+ str(lat) +'),0))')
    
    query_job = client.query(QUERY )    # Start Query API Request
    query_result = query_job.result()  # Get Query Result 
    df = query_result.to_dataframe() 

    return df.to_string(header=False, index=False)

# set OS environment
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=SERVICE_KEY_jSON

client = bigquery.Client()         # Start the BigQuery Client
# QUERY = ('select * from `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` where BOROUGH="BROOKLYN" OR BOROUGH="QUEENS"')
QUERY = ('select * from `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` where date(timestamp) = "2012-07-01" AND (BOROUGH="BROOKLYN" OR BOROUGH="QUEENS")')

query_job = client.query(QUERY )    # Start Query API Request
query_result = query_job.result()  # Get Query Result
df = query_result.to_dataframe() 


# print(df.values[0][7])
# datetime_element = df.values[0][7]
# date_element = datetime_element.date()
# print(date_element)
# df.to_csv('test.csv')
# result_data = []
# row_data = []

# row = 0
# for x in df:
#     collision_date = gimmeADate(df.values[row][7])
#     borough = df.values[row][0]
#     latitude = df.values[row][8]
#     longitude = df.values[row][9]
#     cyclists_injured = df.values[row][11]
#     cyclists_killed = df.values[row][12]
#     motorists_injured = df.values[row][13]
#     motorists_killed = df.values[row][14]
#     ped_injured = df.values[row][15]
#     pes_killed = df.values[row][16]
#     persons_injured = df.values[row][17]
#     persons_killed = df.values[row][18]

#     row = row + 1


    
#     tmp_string = str(df.values[0])
#     tmp_string = tmp_string.split(",")
#     BOROUGH = tmp_string[0]
#     result_data.append(BOROUGH)

#print(df)
# print(result_data)

here = getBoroghFromLatLong(-73.94398, 40.680088)
print(here)