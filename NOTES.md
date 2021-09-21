# Get the earliest record of a traffic collision in the NYPD

```sql
select min(timestamp) from `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions`
```

Results in *2012-07-01T00:05:00*

# mow get the max

```sql
select max(timestamp) from `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions`
```

Results in *2021-09-14T23:57:00*

_we now know the limits of the weather query we shall use_

# Build the query for the weather data into a new view

```sql
CREATE VIEW `uhi-assignment-1.assignment.weather_2012_to_2021` AS
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, wdsp, mxpsd, gust, max, min, prcp, sndp, fog 
FROM `bigquery-public-data.noaa_gsod.gsod2012` WHERE stn='725060'
union all
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, wdsp, mxpsd, gust, max, min, prcp, sndp, fog 
FROM `bigquery-public-data.noaa_gsod.gsod2013` WHERE stn='725060'
union all
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, wdsp, mxpsd, gust, max, min, prcp, sndp, fog 
FROM `bigquery-public-data.noaa_gsod.gsod2014` WHERE stn='725060' AND wban='14756'
union all
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, wdsp, mxpsd, gust, max, min, prcp, sndp, fog 
FROM `bigquery-public-data.noaa_gsod.gsod2015` WHERE stn='725060'
union all
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, wdsp, mxpsd, gust, max, min, prcp, sndp, fog 
FROM `bigquery-public-data.noaa_gsod.gsod2016` WHERE stn='725060'
union all
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, wdsp, mxpsd, gust, max, min, prcp, sndp, fog 
FROM `bigquery-public-data.noaa_gsod.gsod2017` WHERE stn='725060'
union all
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, wdsp, mxpsd, gust, max, min, prcp, sndp, fog 
FROM `bigquery-public-data.noaa_gsod.gsod2018` WHERE stn='725060'
union all
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, wdsp, mxpsd, gust, max, min, prcp, sndp, fog 
FROM `bigquery-public-data.noaa_gsod.gsod2019` WHERE stn='725060'
union all
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, wdsp, mxpsd, gust, max, min, prcp, sndp, fog 
FROM `bigquery-public-data.noaa_gsod.gsod2020` WHERE stn='725060'
union all
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, wdsp, mxpsd, gust, max, min, prcp, sndp, fog 
FROM `bigquery-public-data.noaa_gsod.gsod2021` WHERE stn='725060'
ORDER BY year, mo, da;
```

# now to check the figures look correct
get the count for all years

```sql
SELECT COUNT(*) FROM `uhi-assignment-1.assignment.weather_2012_to_2021`
```
Returns *3549*

So let's check the years using

```sql
SELECT COUNT(*) FROM `uhi-assignment-1.assignment.weather_2012_to_2021` WHERE year = '2012'
```
and adjust the year from 2012 - 2021
This will result in the following:
|Year|Query                                                                                      |Result|
|:--:|  :---:                                                                                    | :---:|
|2012|SELECT COUNT(*) FROM `uhi-assignment-1.assignment.weather_2012_to_2021` WHERE year = '2012'|366   |
|2013|SELECT COUNT(*) FROM `uhi-assignment-1.assignment.weather_2012_to_2021` WHERE year = '2013'|365   |
|2014|SELECT COUNT(*) FROM `uhi-assignment-1.assignment.weather_2012_to_2021` WHERE year = '2014'|365   |
|2015|SELECT COUNT(*) FROM `uhi-assignment-1.assignment.weather_2012_to_2021` WHERE year = '2015'|365   |
|2016|SELECT COUNT(*) FROM `uhi-assignment-1.assignment.weather_2012_to_2021` WHERE year = '2016'|366   |
|2017|SELECT COUNT(*) FROM `uhi-assignment-1.assignment.weather_2012_to_2021` WHERE year = '2017'|365   |
|2018|SELECT COUNT(*) FROM `uhi-assignment-1.assignment.weather_2012_to_2021` WHERE year = '2018'|365   |
|2019|SELECT COUNT(*) FROM `uhi-assignment-1.assignment.weather_2012_to_2021` WHERE year = '2019'|365   |
|2020|SELECT COUNT(*) FROM `uhi-assignment-1.assignment.weather_2012_to_2021` WHERE year = '2020'|366   |
|2021|SELECT COUNT(*) FROM `uhi-assignment-1.assignment.weather_2012_to_2021` WHERE year = '2021'|261   |
|    |                                                                                           |3549  |

The sum total of the years matches the total we got for all years.
(2012,2016,2020 are leap years)

Now to construct our collisions data

# Constructing the collision data

To begin with, extract the number of collisions per day, I have also chosen to extract injuries as this could be usful later
```sql
CREATE VIEW `uhi-assignment-1.assignment.collisions_data` AS
SELECT CAST(timestamp as DATE) as collision_date, 
COUNT(CAST(timestamp as DATE)) as NUM_COLLISIONS, 
SUM(CAST(number_of_cyclist_killed as INT64)) as CYCLISTS_KILLED,
SUM(CAST(number_of_cyclist_injured as INT64)) as CYCLISTS_INJURED,
SUM(CAST(number_of_motorist_killed as INT64)) as MOTORISTS_KILLED,
SUM(CAST(number_of_motorist_injured as INT64)) as MOTORISTS_INJURED,
SUM(CAST(number_of_pedestrians_killed as INT64)) as PEDS_KILLED,
SUM(CAST(number_of_pedestrians_injured as INT64)) as PEDS_INJURED,
SUM(CAST(number_of_persons_killed as INT64)) as PERSONS_KILLED,
SUM(CAST(number_of_persons_injured as INT64)) as PERSONS_INJURED,
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions`
GROUP BY collision_date;
```

In order to check the query, I selected a date and extracted all the records for that date:

```sql
select * from `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` where date(timestamp) = '2021-04-16'
```
I saved this off as a local CSV file and brought this into excel (see file 'data_check_1') I then did a sum on the columns for pedestrian, motorists, etc on killed and injured.  This gave the following:

|Num Records|number_of_cyclist_injured|number_of_cyclist_killed|number_of_motorist_injured|number_of_motorist_killed|number_of_pedestrians_injured|number_of_pedestrians_killed|number_of_persons_injured|number_of_persons_killed|
|:--:       | :--:                    | :--:                   | :--:                     | :--:                    | :--:                        | :--:                       | :--:                    | :--:                   |
|299        |11                	      |0                       |101                       |0                        |16                           |0                           |129                      |0                       |

I then ran the following query which would extract all columns from the newly created view for the same date:

```sql
SELECT * FROM `uhi-assignment-1.assignment.collisions_data` where collision_date = '2021-04-16'
```

|Row|collision_date|NUM_COLLISIONS|CYCLISTS_KILLED|CYCLISTS_INJURED|MOTORISTS_KILLED|MOTORISTS_INJURED|PEDS_KILLED|PEDS_INJURED|PERSONS_KILLED|PERSONS_INJURED|
|:-:|:-:           |:-:           |:-:            |:-:             |:-:             |:-:              | :-:       | :-:        | :-:          | :-:           |
|1  |2021-04-16    |299           |0              |11              |0               |101              |0          |16          |0             |129            |

The order of the columns is different, but as can be seen the values match - so collision data is good.

## Now to add in a day field

```sql
CREATE VIEW `uhi-assignment-1.assignment.collisions_data_final` 
AS SELECT FORMAT_DATE("%u", collision_date) as day, collision_date, NUM_COLLISIONS, CYCLISTS_KILLED, CYCLISTS_INJURED, MOTORISTS_KILLED, MOTORISTS_INJURED, PEDS_KILLED, PEDS_INJURED, PERSONS_KILLED, PERSONS_INJURED
FROM `uhi-assignment-1.assignment.collisions_data`
```

Generates a new view. 

To check the day has been added correctly, 

```sql
SELECT * FROM `uhi-assignment-1.assignment.collisions_data_final`  where collision_date = '2019-12-04'
```
Which shows that the 4th December, 2019 is a Wednesday (insert image)

# Finishing touches

Now we need to collate the collisions data with the weather data.

```sql
CREATE TABLE `uhi-assignment-1.assignment.collated_data` 
AS SELECT day, year, mo, da, collision_date, temp, dewp, slp, visib, wdsp, mxpsd, gust, max, min, prcp, sndp, fog, CYCLISTS_KILLED, CYCLISTS_INJURED, MOTORISTS_KILLED, MOTORISTS_INJURED, PEDS_KILLED, PEDS_INJURED,PERSONS_KILLED, PERSONS_INJURED, NUM_COLLISIONS FROM `uhi-assignment-1.assignment.weather_2012_to_2021` as weather,
`uhi-assignment-1.assignment.collisions_data_final` as complaints WHERE complaints.collision_date = weather.date
```

Data has been stored in "Final_Data" directory