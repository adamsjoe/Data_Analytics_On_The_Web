import csv
import sys
from datetime import datetime

# constants
FILENAME_IN = 'Final_Data_Collated.csv'
FILENAME_OUT = 'updated_Final_Data_Collated.csv'


# Entry point
format=  "%d-%m-%Y"

toCheckFor = '-'
toReplaceWith = '/'

with open(FILENAME_IN, mode='r') as in_file, \
     open(FILENAME_OUT, mode='w') as out_file:

    for row in in_file:
        DATE,BOROUGH,WEEKDAY,YEAR,MONTH,DAY,COLLISION_DATE,TEMP,DEWP,SLP,VISIB,WDSP,MXPSD,GUST,MAX,MIN,PRCP,SNDP,FOG,CYC_KILL,CYC_INJD,MOTO_KILL,MOTO_INJD,PEDS_KILL,PEDS_INJD,PERS_KILL,PERS_INJD,NUM_COLS = row.split(',')
        print(f'Looking at the date: {DATE}')
        # check if '-' is present
        if toCheckFor in DATE:
            print(f'  --> {toCheckFor} was found in {DATE}')
            # if present, then repace
            DATE = DATE.replace(toCheckFor, toReplaceWith)
            print(f'  --> Date is now {DATE}')
        else:
            print(f'  --> {DATE} is does not have the demonic {toCheckFor} character')

        # now check format
        try:
            datetime.strptime(DATE, format)
        except ValueError:
            print('  --> wrong format')
            temp = DATE.split(toReplaceWith)
            DATE = (f'{temp[2]}-{temp[1]}-{temp[0]}')
            # print(f'{temp}')
    
        # out to the file
        out_file.write(f'{DATE},{BOROUGH},{WEEKDAY},{YEAR},{MONTH},{DAY},{COLLISION_DATE},{TEMP},{DEWP},{SLP},{VISIB},{WDSP},{MXPSD},{GUST},{MAX},{MIN},{PRCP},{SNDP},{FOG},{CYC_KILL},{CYC_INJD},{MOTO_KILL},{MOTO_INJD},{PEDS_KILL},{PEDS_INJD},{PERS_KILL},{PERS_INJD},{NUM_COLS}')

# close file handles
in_file.close()
out_file.close()