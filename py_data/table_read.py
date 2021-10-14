import os
import modin.pandas as pd
from google.cloud import bigquery
from datetime import datetime

# constants
SERVICE_KEY_jSON = './service_key.json'

# set OS environment
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=SERVICE_KEY_jSON

client = bigquery.Client()         # Start the BigQuery Client
QUERY = ('SELECT * FROM `uhi-assignment-1.assignment.collated_data`')

query_job = client.query(QUERY)    # Start Query API Request
query_result = query_job.result()  # Get Query Result
df = query_result.to_dataframe() 

# print(df)
for ind in df.index:
    borough           = df['borough'][ind]

    if borough = "BB":
        # we need to do fancy shit
        