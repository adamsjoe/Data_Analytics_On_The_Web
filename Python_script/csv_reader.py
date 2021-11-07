import csv
import sys
import os
from google.cloud import bigquery

# constants
SERVICE_KEY_jSON = './service_key.json'
FILENAME_IN = 'bq-results-2017-jan.csv'
FILENAME_OUT = 'Final_data_2017-jan.csv'

# set OS environment
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_KEY_jSON


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


def open_file(file_in, skip_header=True):
    print(f'Opening file {file_in}')
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
    x = 1
    unknown_rows = 0
    found_rows = 0
    linesInFile = len(data_in)

    print(f'Processing {linesInFile} lines')

    output_dict = {}

    for row in data_in:
        # make a little progress counter -
        # otherwise we have no clue what's happening
        pcent = round((x / linesInFile * 100), 2)
        print(f'Processing row - {x} - {pcent}%')

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

        # someone forgot to fill in numbers, so we
        # need to check something is here as there
        # are nulls in the data
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

        # deal with the horrid "BB" - these are entries which have no latitude, longitude or borough.
        if neighborhood == "BB":
            neighborhood = getBoroghFromLatLong(lat, long)
            print(
                f'     --> Handling missing borogh - from {lat} and {long} got {neighborhood}')
            found_rows += 1
        else:
            neighborhood = neighborhood

        # we have bad data we cannot do anything with, I've marked this as
        # neighbourhood "Unknown" - skip over this
        if neighborhood == "Unknown":
            print('     Skipping row - unusable data')
            unknown_rows += 1
            continue

        if neighborhood not in output_dict[collision_date]:
            output_dict[collision_date][neighborhood] = {}
            output_dict[collision_date][neighborhood]["day"] = ""
            output_dict[collision_date][neighborhood]["year"] = ""
            output_dict[collision_date][neighborhood]["mo"] = ""
            output_dict[collision_date][neighborhood]["da"] = ""

            output_dict[collision_date][neighborhood]["temp"] = 0
            output_dict[collision_date][neighborhood]["dewp"] = 0
            output_dict[collision_date][neighborhood]["slp"] = 0
            output_dict[collision_date][neighborhood]["visib"] = 0
            output_dict[collision_date][neighborhood]["wdsp"] = 0
            output_dict[collision_date][neighborhood]["mxpsd"] = 0
            output_dict[collision_date][neighborhood]["gust"] = 0
            output_dict[collision_date][neighborhood]["max"] = 0
            output_dict[collision_date][neighborhood]["min"] = 0
            output_dict[collision_date][neighborhood]["prcp"] = 0
            output_dict[collision_date][neighborhood]["sndp"] = 0
            output_dict[collision_date][neighborhood]["fog"] = 0

            output_dict[collision_date][neighborhood]["cyclists_killed"] = 0
            output_dict[collision_date][neighborhood]["cyclists_injured"] = 0
            output_dict[collision_date][neighborhood]["motorists_killed"] = 0
            output_dict[collision_date][neighborhood]["motorists_injured"] = 0
            output_dict[collision_date][neighborhood]["peds_killed"] = 0
            output_dict[collision_date][neighborhood]["peds_injured"] = 0
            output_dict[collision_date][neighborhood]["persons_killed"] = 0
            output_dict[collision_date][neighborhood]["persons_injured"] = 0
            output_dict[collision_date][neighborhood]["num_collisions"] = 0

        # get the ccurrent things we want to deal with
        current_temp = temp
        current_dewp = dewp
        current_slp = slp
        current_visib = visib
        current_wdsp = wdsp
        current_mxpsd = mxpsd
        current_gust = gust
        current_max = max
        current_min = min
        current_prcp = prcp
        current_sndp = sndp
        current_fog = fog
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
        output_dict[collision_date][neighborhood]["da"] = da
        output_dict[collision_date][neighborhood]["temp"] = current_temp
        output_dict[collision_date][neighborhood]["dewp"] = current_dewp
        output_dict[collision_date][neighborhood]["slp"] = current_slp
        output_dict[collision_date][neighborhood]["visib"] = current_visib
        output_dict[collision_date][neighborhood]["wdsp"] = current_wdsp
        output_dict[collision_date][neighborhood]["mxpsd"] = current_mxpsd
        output_dict[collision_date][neighborhood]["gust"] = current_gust
        output_dict[collision_date][neighborhood]["max"] = current_max
        output_dict[collision_date][neighborhood]["min"] = current_min
        output_dict[collision_date][neighborhood]["prcp"] = current_prcp
        output_dict[collision_date][neighborhood]["sndp"] = current_sndp
        output_dict[collision_date][neighborhood]["fog"] = current_fog
        output_dict[collision_date][neighborhood]["cyclists_killed"] = update_cyclists_killed
        output_dict[collision_date][neighborhood]["cyclists_injured"] = update_cyclists_injured
        output_dict[collision_date][neighborhood]["motorists_killed"] = update_motorists_killed
        output_dict[collision_date][neighborhood]["motorists_injured"] = update_motorists_injured
        output_dict[collision_date][neighborhood]["peds_killed"] = update_peds_killed
        output_dict[collision_date][neighborhood]["peds_injured"] = update_peds_injured
        output_dict[collision_date][neighborhood]["persons_killed"] = update_persons_killed
        output_dict[collision_date][neighborhood]["persons_injured"] = update_persons_injured
        output_dict[collision_date][neighborhood]["num_collisions"] = update_num_collisions
        x += 1

    print(f'Found {found_rows} rows, but had to drop {unknown_rows} rows')
    return output_dict


# Entry point
data = open_file(FILENAME_IN)
contents = collate_data(data)

# headers for use in the CSV output
headers = ['DATE', 'BOROUGH', 'WEEKDAY', 'YEAR', 'MONTH', 'DAY', 'COLLISION_DATE', 'TEMP', 'DEWP', 'SLP', 'VISIB', 'WDSP', 'MXPSD', 'GUST', 'MAX', 'MIN', 'PRCP', 'SNDP', 'FOG', 'CYC_KILL', 'CYC_INJD', 'MOTO_KILL', 'MOTO_INJD', 'PEDS_KILL', 'PEDS_INJD', 'PERS_KILL', 'PERS_INJD', 'NUM_COLS']

# output list
output = []

# print the headers on screen (formatted) so we know things have happened
print(
    "{:<15} {:<15} {:<8} {:<8} {:<8} {:<8} {:<15} {:<8} {:<8}   {:<8}  {:<8}    {:<8}   {:<8}    {:<8}   {:<8}  {:<8}  {:<8}   {:<8}   {:<8}  {:<10}      {:<10}      {:<15}       {:<10}       {:<10}      {:10}        {:<10}       {:<10}     {:<10}".format
    ('DATE', 'BOROUGH', 'WEEKDAY', 'YEAR', 'MONTH', 'DAY', 'COLLISION_DATE',
     'TEMP', 'DEWP', 'SLP', 'VISIB', 'WDSP', 'MXPSD', 'GUST', 'MAX', 'MIN',
     'PRCP', 'SNDP', 'FOG', 'CYC_KILL', 'CYC_INJD', 'MOTO_KILL', 'MOTO_INJD',
     'PEDS_KILL', 'PEDS_INJD', 'PERS_KILL', 'PERS_INJD', 'NUM_COLS')
    )

for key in contents:
    row_date = ""
    subkeys = ""

    # print(key) #gives me dates
    row_date = key

    # print(len(contents[key]))

    for subkeys in contents[key]:

        # print(subkeys)
        # print(row_date + "  " + subkeys)
        dayVal = contents[key][subkeys].get("day")

        yearVal = contents[key][subkeys].get("year")
        monthVal = contents[key][subkeys].get("mo")
        daVal = contents[key][subkeys].get("da")

        coll_date = row_date

        tempVal = contents[key][subkeys].get("temp")
        dewpVal	= contents[key][subkeys].get("dewp")
        slpVal = contents[key][subkeys].get("slp")
        visibVal = contents[key][subkeys].get("visib")
        wdspVal = contents[key][subkeys].get("wdsp")
        mxpsdVal = contents[key][subkeys].get("mxpsd")
        gustVal = contents[key][subkeys].get("gust")
        maxVal = contents[key][subkeys].get("max")
        minVal = contents[key][subkeys].get("min")
        prcpVal = contents[key][subkeys].get("prcp")
        sndpVal = contents[key][subkeys].get("sndp")
        fogVal = contents[key][subkeys].get("fog")

        cyclists_killedVal = contents[key][subkeys].get("cyclists_killed")
        cyclists_injuredVal = contents[key][subkeys].get("cyclists_injured")

        motorists_killedVal = contents[key][subkeys].get("motorists_killed")
        motorists_injuredVal = contents[key][subkeys].get("motorists_injured")

        peds_killedVal = contents[key][subkeys].get("peds_killed")
        peds_injuredVal = contents[key][subkeys].get("peds_injured")

        persons_killedVal = contents[key][subkeys].get("persons_killed")
        persons_injuredVal = contents[key][subkeys].get("persons_injured")

        num_collisionsVa = contents[key][subkeys].get("num_collisions")

        item = row_date, subkeys, dayVal, yearVal, monthVal, daVal, coll_date, tempVal, dewpVal, slpVal, visibVal, wdspVal, mxpsdVal, gustVal, maxVal, minVal, prcpVal, sndpVal, fogVal, cyclists_killedVal, cyclists_injuredVal, motorists_killedVal, motorists_injuredVal, peds_killedVal, peds_injuredVal, peds_killedVal, persons_injuredVal, num_collisionsVa

        output.append(item)

        print(
            "{:<15}  {:<15}     {:<8}      {:<8}   {:<8}    {:<8}  {:<15}            {:<8}   {:<8}   {:<8}  {:<8}    {:<8}   {:<8}    {:<8}   {:<8}  {:<8}  {:<8}   {:<8}   {:<8}  {:<10}      {:<10}      {:<15}       {:<10}       {:<10}      {:10}        {:<10}       {:<10}     {:<10}".format
            (row_date, subkeys, dayVal, yearVal, monthVal, daVal, coll_date, tempVal, dewpVal, slpVal, visibVal, wdspVal, mxpsdVal, gustVal, maxVal, minVal, prcpVal, sndpVal, fogVal, cyclists_killedVal, cyclists_injuredVal, motorists_killedVal, motorists_injuredVal, peds_killedVal, peds_injuredVal, peds_killedVal, persons_injuredVal, num_collisionsVa)
            )

create_report(FILENAME_OUT, headers, output)
