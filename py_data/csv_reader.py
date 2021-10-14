import csv
import sys
import pprint
from google.cloud import bigquery
pp = pprint.PrettyPrinter(indent=4)
import os

# constants
SERVICE_KEY_jSON = './service_key.json'
FILENAME_IN = 'results.csv'
FILENAME_OUT = 'Final_data_v2.csv'

# usdd for the csv output
csv_headers = ['day','year','mo','da','collision_date','temp','dewp','slp','visib','wdsp','mxpsd','gust','max','min','prcp','sndp','fog','CYCLISTS_KILLED','CYCLISTS_INJURED','MOTORISTS_KILLED','MOTORISTS_INJURED','PEDS_KILLED','PEDS_INJURED','PERSONS_KILLED','PERSONS_INJURED','NUM_COLLISIONS']

# set OS environment
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=SERVICE_KEY_jSON

def getBoroghFromLatLong(lat, long):
    client = bigquery.Client()         # Start the BigQuery Client
    QUERY = ('SELECT UPPER(borough) FROM `bigquery-public-data.current_york_taxi_trips.taxi_zone_geom` tz_loc WHERE (ST_DWithin(tz_loc.zone_geom, ST_GeogPoint('+ str(long) +', '+ str(lat) +'),0))')
    
    query_job = client.query(QUERY)    # Start Query API Request
    query_result = query_job.result()  # Get Query Result 
    df = query_result.to_dataframe() 

def open_file(file_in, skip_header=True):
    # setup a variable to hold the data from the file
    data = ''
    try:
        with open(file_in, encoding='utf-8-sig', mode='r') as file:
            reader = csv.reader(file)
            if skip_header is True:
                # if skip_header is true, then skip to the next line
                next(reader, None)
            # make each row in the input file into a tuple and add
            # this to a list to be returned.
            data = [tuple(row) for row in reader]
    # handle file not found errors with a nice error message
    except FileNotFoundError:
        print('File {} does not exist.'.format(file_in))
        sys.exit(1)
    except:  # generic catch all error message
        print(
            'Trying to open {} failed.  No further information was available'
            .format(file_in))
        sys.exit(1)
    # return the data variable (the file contents)
    return data

def create_report(file_name, headers, content):
    try:
        with open(file_name, 'a', newline="") as out_file:
            csvwriter = csv.writer(out_file)
            csvwriter.writerow(headers)
            csvwriter.writerows(content)
    except:
        print(
            'Trying to create {} failed.  No further information was available'
            .format(file_name)
        )
        sys.exit(1)

def collate_data(data_in):
    output_dict = {}
    x = 0
    for row in data_in:
        # make things easier to reference
        day = row[0]
        year = row[1]
        mo = row[2]
        da = row[3]
        collision_date = row[4]
        neighborhood = row[5]
        lat = row[6]
        long = row[7]
        temp = row[8]
        dewp = row[9]
        slp = row[10]
        visib = row[11]
        wdsp = row[12]
        mxpsd = row[13]
        gust = row[14]
        max = row[15]
        min = row[16]
        prcp = row[17]
        sndp = row[18]
        fog = row[19]
        if row[20] == "":
            cyclists_killed = 0
        else:
            cyclists_killed = int(row[20])
        
        if row[21] == "":
            cyclists_injured = int(row[21])
        else:
            cyclists_injured = int(row[21])

        if row[22] == "":
            motorists_killed = 0
        else:            
            motorists_killed = int(row[22])
        
        if row[23] == "":
            motorists_injured = 0
        else:
            motorists_injured = int(row[23])
        
        if row[24] == "":
            peds_killed = 0
        else:
            peds_killed = int(row[24])
        
        if row[25] == "":
            peds_injured = 0
        else:
            peds_injured = int(row[25])

        if row[26] == "":
                persons_killed = 0
        else:
            persons_killed = int(row[26])
        
        if row[27] == "":
            persons_injured = 0
        else:
            persons_injured = int(row[27])

        if row[28] == "":
            num_collisions = 0
        else:
            num_collisions = int(row[28])

        if collision_date not in output_dict:
            output_dict[collision_date] = {}

        # we have bad data we cannot do anything with, I've marked this as neighbourhood "Uknown" - skip over this
        if neighborhood == "Unknown":
            continue
        # # deal with the horrid "BB" - the uknown neighbourhoods:
        # if neighborhood == "BB":
        #     neighbourhood = getBoroghFromLatLong(lat, long)
        # else:
        #     neighborhood = neighborhood
    
        
        if neighborhood not in output_dict[collision_date]:
            output_dict[collision_date][neighborhood]  = {}
            output_dict[collision_date][neighborhood]["day"]  = ""
            output_dict[collision_date][neighborhood]["year"]  = ""
            output_dict[collision_date][neighborhood]["mo"]  = ""
            output_dict[collision_date][neighborhood]["da"]  = ""

            output_dict[collision_date][neighborhood]["temp"]  = 0
            output_dict[collision_date][neighborhood]["dewp"]  = 0
            output_dict[collision_date][neighborhood]["slp"]  = 0
            output_dict[collision_date][neighborhood]["visib"]  = 0
            output_dict[collision_date][neighborhood]["wdsp"]  = 0
            output_dict[collision_date][neighborhood]["mxpsd"]  = 0
            output_dict[collision_date][neighborhood]["gust"]  = 0
            output_dict[collision_date][neighborhood]["max"]  = 0
            output_dict[collision_date][neighborhood]["min"]  = 0
            output_dict[collision_date][neighborhood]["prcp"]  = 0
            output_dict[collision_date][neighborhood]["sndp"]  = 0
            output_dict[collision_date][neighborhood]["fog"]  = 0
            output_dict[collision_date][neighborhood]["cyclists_killed"]  = 0
            output_dict[collision_date][neighborhood]["cyclists_injured"]  = 0
            output_dict[collision_date][neighborhood]["motorists_killed"]  = 0
            output_dict[collision_date][neighborhood]["motorists_injured"]  = 0
            output_dict[collision_date][neighborhood]["peds_killed"]  = 0
            output_dict[collision_date][neighborhood]["peds_injured"]  = 0
            output_dict[collision_date][neighborhood]["persons_killed"]  = 0
            output_dict[collision_date][neighborhood]["persons_injured"]  = 0
            output_dict[collision_date][neighborhood]["num_collisions"]  = 0
        
        # get the ccurrent things we want to deal with
        current_temp = output_dict[collision_date][neighborhood].get("temp")
        current_dewp = output_dict[collision_date][neighborhood].get("dewp")
        current_slp = output_dict[collision_date][neighborhood].get("slp")
        current_visib = output_dict[collision_date][neighborhood].get("visib")
        current_wdsp = output_dict[collision_date][neighborhood].get("wdsp")
        current_mxpsd = output_dict[collision_date][neighborhood].get("mxpsd")
        current_gust = output_dict[collision_date][neighborhood].get("gust")
        current_max = output_dict[collision_date][neighborhood].get("max")
        current_min = output_dict[collision_date][neighborhood].get("min")
        current_prcp = output_dict[collision_date][neighborhood].get("prcp")
        current_sndp = output_dict[collision_date][neighborhood].get("sndp")
        current_fog = output_dict[collision_date][neighborhood].get("fog")
        current_cyclists_killed = int(output_dict[collision_date][neighborhood].get("cyclists_killed"))
        current_cyclists_injured = output_dict[collision_date][neighborhood].get("cyclists_injured")
        current_motorists_killed = output_dict[collision_date][neighborhood].get("motorists_killed")
        current_motorists_injured = output_dict[collision_date][neighborhood].get("motorists_injured")
        current_peds_killed = output_dict[collision_date][neighborhood].get("peds_killed")
        current_peds_injured = output_dict[collision_date][neighborhood].get("peds_injured")
        current_persons_killed = output_dict[collision_date][neighborhood].get("persons_killed")
        current_persons_injured = output_dict[collision_date][neighborhood].get("persons_injured")
        current_num_collisions = output_dict[collision_date][neighborhood].get("num_collisions")

        # now we have what values are in the current row - tile to update
        update_temp = float(temp) + float(current_temp)
        update_dewp = float(dewp) + float(current_dewp)
        update_slp = float(slp) + float(current_slp)
        update_visib = float(visib) + float(current_visib)
        update_wdsp = float(wdsp) + float(current_wdsp)
        update_mxpsd = float(mxpsd) + float(current_mxpsd)
        update_gust = float(gust) + float(current_gust)
        update_max = float(max) + float(current_max)
        update_min = float(min) + float(current_min)
        update_prcp = float(prcp) + float(current_prcp)
        update_sndp = float(sndp) + float(current_sndp)
        update_fog = int(fog) + int(current_fog)
        update_cyclists_killed = cyclists_killed + current_cyclists_killed
        update_cyclists_injured = cyclists_injured + current_cyclists_injured
        update_motorists_killed = motorists_killed + current_motorists_killed
        update_motorists_injured = motorists_injured + current_motorists_injured
        update_peds_killed = peds_killed + current_peds_killed
        update_peds_injured = peds_injured + current_peds_injured
        update_persons_killed = persons_killed + current_persons_killed
        update_persons_injured = persons_injured + current_persons_injured
        update_num_collisions = num_collisions + current_num_collisions

        # now to apply these all to our dictionary
        output_dict[collision_date][neighborhood]["day"] = day
        output_dict[collision_date][neighborhood]["year"] = year
        output_dict[collision_date][neighborhood]["mo"] = mo
        output_dict[collision_date][neighborhood]["day"] = da
        output_dict[collision_date][neighborhood]["temp"]  = update_temp
        output_dict[collision_date][neighborhood]["dewp"]  = update_dewp
        output_dict[collision_date][neighborhood]["slp"]  = update_slp
        output_dict[collision_date][neighborhood]["visib"]  = update_visib
        output_dict[collision_date][neighborhood]["wdsp"]  = update_wdsp
        output_dict[collision_date][neighborhood]["mxpsd"]  = update_mxpsd
        output_dict[collision_date][neighborhood]["gust"]  = update_gust
        output_dict[collision_date][neighborhood]["max"]  = update_max
        output_dict[collision_date][neighborhood]["min"]  = update_min
        output_dict[collision_date][neighborhood]["prcp"]  = update_prcp
        output_dict[collision_date][neighborhood]["sndp"]  = update_sndp
        output_dict[collision_date][neighborhood]["fog"]  = update_fog
        output_dict[collision_date][neighborhood]["cyclists_killed"]  = update_cyclists_killed
        output_dict[collision_date][neighborhood]["cyclists_injured"]  = update_cyclists_injured
        output_dict[collision_date][neighborhood]["motorists_killed"]  = update_motorists_killed
        output_dict[collision_date][neighborhood]["motorists_injured"]  = update_motorists_injured
        output_dict[collision_date][neighborhood]["peds_killed"]  = update_peds_killed
        output_dict[collision_date][neighborhood]["peds_injured"]  = update_peds_injured
        output_dict[collision_date][neighborhood]["persons_killed"]  = update_persons_killed
        output_dict[collision_date][neighborhood]["persons_injured"]  = update_persons_injured
        output_dict[collision_date][neighborhood]["num_collisions"]  = update_num_collisions
            
    return output_dict
    # x+=1
    # print(x)
    # pp.pprint(output_dict)


data = open_file(FILENAME_IN)
contents = collate_data(data)
create_report(FILENAME_OUT, csv_headers, contents)
