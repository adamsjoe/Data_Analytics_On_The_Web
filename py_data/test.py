import os
import pandas as pd
from google.cloud import bigquery
import re

# constands
SERVICE_KEY_jSON = 'service_key.json'

# set OS environment
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=SERVICE_KEY_jSON

def extractDateFromArray(dateFrameIn, alias):
    
    DATE_FRAME_START="'["
    DATE_FRAME_END="T"

    # extract the value we need and convet to a string
    tmp_data = str(dateFrameIn[alias].values)

    # we now need to extract the date
    dateString = tmp_data
    return dateString

client = bigquery.Client()         # Start the BigQuery Client
MIN_DATE_QUERY = ('select min(timestamp) AS MIN_DATE from `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions`')

query_job = client.query(MIN_DATE_QUERY )    # Start Query API Request
query_result = query_job.result()  # Get Query Result
df = query_result.to_dataframe() 
# tmp_min_date = str(df["MIN_DATE"].values)
# tmp_min_date = tmp_min_date.split("T")
# min_date = tmp_min_date[0]

# print(min_date)

# print(">>" + str(df[0]) + "<<")
# print(type(df))
# print(df.columns)
# print(df["MIN_DATE"].values)
x=extractDateFromArray(df, "MIN_DATE")
print(x)

