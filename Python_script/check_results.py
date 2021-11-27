import csv
import sys
import os
from datetime import datetime
from google.cloud import bigquery

# constants
SERVICE_KEY_jSON = './service_key.json'
FILENAME_IN = 'pred_check_results.csv'
FILENAME_OUT = 'updated_Final_Data_Collated.csv'

# set OS environment
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_KEY_jSON


toCheckFor = '-'
toReplaceWith = '/'

def getBoroghFromLatLong(lat, long):
    print('Entered Borough Query')
    returnVal = ""
    client = bigquery.Client()         # Start the BigQuery Client
    QUERY = (
        'SELECT UPPER(borough) AS BOROUGH FROM `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` tz_loc WHERE (ST_DWithin(tz_loc.zone_geom, ST_GeogPoint(' + str(long) + ', ' + str(lat) + '),0))')

    query_job = client.query(QUERY)    # Start Query API Request
    query_result = query_job.result()  # Get Query Result
    df = query_result.to_dataframe()

    try:
        returnVal = str(df["BOROUGH"].values[0])
    except IndexError:
        returnVal = "Unknown"

    return returnVal

with open(FILENAME_IN, mode='r') as in_file, \
     open(FILENAME_OUT, mode='w') as out_file:

    for row in in_file:
        borough,contributing_factor_vehicle_1, contributing_factor_vehicle_2,contributing_factor_vehicle_3,contributing_factor_vehicle_4,contributing_factor_vehicle_5,cross_street_name,timestamp,latitude,longitude,number_of_cyclist_injured,number_of_cyclist_killed,number_of_motorist_injured,number_of_motorist_killed,number_of_pedestrians_injured,number_of_pedestrians_killed,number_of_persons_injured,number_of_persons_killed,off_street_name,on_street_name,unique_key,vehicle_type_code1,vehicle_type_code2,vehicle_type_code_3,vehicle_type_code_4,vehicle_type_code_5,zip_code = row.split(',')
        print(f'Looking at the BOROUGH: {borough} ')
        
        if borough == "":
            print(f'  --> borough was empty')
            print(f'    --> using lat of  :{latitude}')
            print(f'    --> using long of :{longitude}')
            # handle any missing lat and longs
            if latitude == "":
                borough = "UNKNOWN"
            else:            
                borough = getBoroghFromLatLong(latitude, longitude)
            print(f'  --> Borough is now {borough}')
        else:
            print(f'  --> {borough} was not empty')


            
        # out to the file
        out_file.write(f'{borough},{contributing_factor_vehicle_1}, {contributing_factor_vehicle_2},{contributing_factor_vehicle_3},{contributing_factor_vehicle_4},{contributing_factor_vehicle_5},{cross_street_name},{timestamp},{latitude},{longitude},{number_of_cyclist_injured},{number_of_cyclist_killed},{number_of_motorist_injured},{number_of_motorist_killed},{number_of_pedestrians_injured},{number_of_pedestrians_killed},{number_of_persons_injured},{number_of_persons_killed},{off_street_name},{on_street_name},{unique_key},{vehicle_type_code1},{vehicle_type_code2},{vehicle_type_code_3},{vehicle_type_code_4},{vehicle_type_code_5},{zip_code}')
        
# close file handles
in_file.close()
out_file.close()