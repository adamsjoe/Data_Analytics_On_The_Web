import os
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import pprint

# constants
SERVICE_KEY_jSON = './service_key.json'

def gimmeADate(dfValIn):

    tmp_date = dfValIn
    date_element = tmp_date.date()    
    return str(date_element)

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
QUERY = ('select * from `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` where date(timestamp) = "2012-07-01" AND (BOROUGH="BROOKLYN" OR BOROUGH="QUEENS")')
#QUERY = ('select * from `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` where date(timestamp) = "2012-07-01" AND BOROUGH="QUEENS"')

query_job = client.query(QUERY)    # Start Query API Request
query_result = query_job.result()  # Get Query Result
df = query_result.to_dataframe() 

result_dictionary = {}
borough_dictionary = {}


for ind in df.index:
    # print(df)
    collision_date = gimmeADate(df['timestamp'][ind])
    borough           = df['borough'][ind]
    latitude          = df['latitude'][ind]
    longitude         = df['longitude'][ind]
    cyclists_injured  = df['number_of_cyclist_injured'][ind]
    cyclists_killed   = df['number_of_cyclist_killed'][ind]
    motorists_injured = df['number_of_motorist_injured'][ind]
    motorists_killed  = df['number_of_motorist_killed'][ind]
    ped_injured       = df['number_of_pedestrians_injured'][ind]
    ped_killed        = df['number_of_pedestrians_killed'][ind]
    persons_injured   = df['number_of_persons_injured'][ind]
    persons_killed    = df['number_of_persons_killed'][ind]

    if collision_date not in result_dictionary:
        # add the date to the result doctionary - and create a new list
        print(f'Adding new date {collision_date} - Adding')
        result_dictionary[collision_date] = {}
    # else:
    #     # the date is already in results_dictionary, so now we just need to calculate things
    #     print("Date already present")        

    # check if the borough exists, if not add a new entry
    if borough not in result_dictionary[collision_date]:
        #print(f'{borough} was not found - Adding')
       
        if borough:
            # we have a borough
            borough = borough
        elif not borough:
            if (not latitude) and (not latitude):
                borough = getBoroghFromLatLong(latitude, longitude)        
            else:
                borough = "UNKNOWN"            
        
        borough_dictionary["cyclists_injured"] = 0
        borough_dictionary["cyclists_killed"] = 0

        borough_dictionary["motorists_injured"] = 0
        borough_dictionary["motorists_killed"] = 0

        borough_dictionary["ped_injured"] = 0
        borough_dictionary["ped_killed"] = 0

        borough_dictionary["persons_injured"] = 0
        borough_dictionary["persons_killed"] = 0

        result_dictionary[collision_date][borough] = borough_dictionary
    #else:
        #print(f'{borough} was found')

    # we not add our results to our borough dictionary

    # first, get the values we have for this date:
    temp_cycle_injured = result_dictionary[collision_date][borough].get("cyclists_injured")
    temp_cycle_killed = result_dictionary[collision_date][borough].get("cyclists_killed")

    temp_motorists_injured = result_dictionary[collision_date][borough].get("motorists_injured")
    temp_motorists_killed = result_dictionary[collision_date][borough].get("motorists_killed")

    temp_ped_injured = result_dictionary[collision_date][borough].get("ped_injured")
    temp_ped_killed = result_dictionary[collision_date][borough].get("ped_killed")

    temp_persons_injured = result_dictionary[collision_date][borough].get("persons_injured")
    temp_persons_killed = result_dictionary[collision_date][borough].get("persons_killed")

    # work out new values
    new_cycle_injured = cyclists_injured + temp_cycle_injured
    new_cycle_killed = cyclists_killed + temp_cycle_killed

    new_motorists_injured = motorists_injured + temp_motorists_injured
    new_motorists_killed = motorists_killed + temp_motorists_killed

    new_ped_injured = ped_injured + temp_ped_injured
    new_ped_killed = ped_killed + temp_ped_killed

    new_persons_injured = persons_injured + temp_persons_injured
    new_persons_killed = persons_killed + temp_persons_killed

    # now to put these values into the dictionary
    # THIS IS UPDATING BOTH BOROUGHS EVEN THOUGH ONLY ONE IS SET??S
    result_dictionary[collision_date][borough]["cyclists_injured"] = new_cycle_injured
    result_dictionary[collision_date][borough]["cyclists_killed"] = new_cycle_killed

    result_dictionary[collision_date][borough]["motorists_injured"] = new_motorists_injured
    result_dictionary[collision_date][borough]["motorists_killed"] = new_motorists_killed

    result_dictionary[collision_date][borough]["ped_injured"] = new_ped_injured
    result_dictionary[collision_date][borough]["ped_killed"] = new_ped_killed

    result_dictionary[collision_date][borough]["persons_injured"] = new_persons_injured
    result_dictionary[collision_date][borough]["persons_killed"] = new_persons_killed


pp = pprint.PrettyPrinter(indent=4)
pp.pprint(result_dictionary)

