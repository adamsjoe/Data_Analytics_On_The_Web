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

I wanted to try to find out if I could extract the neigbourhood, so I first found how many bourghs there are in the dataset:
```sql
select distinct (borough) FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions`;
```
which yielded the following:
|Row|bourgh       |
|1	|BROOKLYN     |
|2  |BRONX        |
|3	|QUEENS       |
|4	|MANHATTAN    |
|5	|STATEN ISLAND|
|6  |null         |

We should get counts of these to ensure we have the correct results later
```sql
select count(*) FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` WHERE borough = "BROOKLYN";
select count(*) FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` WHERE borough = "BRONX";
select count(*) FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` WHERE borough = "QUEENS";
select count(*) FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` WHERE borough = "MANHATTAN";
select count(*) FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` WHERE borough = "STATEN ISLAND";
select count(*) FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` WHERE borough is null;
```
This gives the following values:
|Row|bourgh       |Count  |
|1	|BROOKLYN     |397447 |
|2  |BRONX        |183043 |
|3	|QUEENS       |338237 |
|4	|MANHATTAN    |291061 |
|5	|STATEN ISLAND|53245  |
|6  |null         |563929 |
|   |             |1826962|

To verify this is correct, we should count how many rows we have in the dataset:
```sql
select count(*) FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions`
```
*1826962* which is a match for the sum of the bourghs

At this point we can do some analysis on the data, we can work out the percentages:
|Row|bourgh       |Count  |Percentage      |
|1	|BROOKLYN     |397447 |21.7545301982198|
|2  |BRONX        |183043 |10.0189823324185|
|3	|QUEENS       |338237 |18.5136308253812|
|4	|MANHATTAN    |291061 |15.9314205768921|
|5	|STATEN ISLAND|53245  |2.91440106581308|
|6  |null         |563929 |30.8670350012753|

We can see that, as a percentage, Staten Island amounts for just under 3% of all the accidents in our dataset.  At this point, we could decide that with such a small amount of the total occuring here that we could discard this. Howver, the "null" bourghs could contain some accidents which occured in the bourgh of sStaten Island (and it could also contain accidents for the other bourghs too.)  So before deciding to discard this, could somehow find a way to determine the bourgh where no bourgh was recorded?
We do have latitude and longitude, but simply running:
```sql
select * from `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` WHERE borough is null
```
shows some latitude and longitude with null values, so let's check this further:
```sql
select * from `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` WHERE borough is null and (latitude is null or longitude is null)
```
This returns 181268 rows - this comes to to approx 9.9% of our records which we have no bourgh, latitude and longitude for ((181268 ÷ 1826962) * 100)

Turning our attendtion to those which do not have null values for latitude or longitude:
```sql
select * from `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` WHERE borough is null and (latitude is null or longitude is not null)
```
This returns 382661 rows.
382661 + 181268 = 563929, which matches our orignal count or null bourghs.  Our data is still correct.

We can then create a view which contains the collision data for the bourghs we know - we do this with the following
```sql
CREATE VIEW `uhi-assignment-1.assignment.collisions_data_bourgh` AS
SELECT CAST(timestamp as DATE) as collision_date, 
COUNT(CAST(timestamp as DATE)) as NUM_COLLISIONS, 
CAST(borough as STRING) as NEIGH,
SUM(CAST(number_of_cyclist_killed as INT64)) as CYCLISTS_KILLED,
SUM(CAST(number_of_cyclist_injured as INT64)) as CYCLISTS_INJURED,
SUM(CAST(number_of_motorist_killed as INT64)) as MOTORISTS_KILLED,
SUM(CAST(number_of_motorist_injured as INT64)) as MOTORISTS_INJURED,
SUM(CAST(number_of_pedestrians_killed as INT64)) as PEDS_KILLED,
SUM(CAST(number_of_pedestrians_injured as INT64)) as PEDS_INJURED,
SUM(CAST(number_of_persons_killed as INT64)) as PERSONS_KILLED,
SUM(CAST(number_of_persons_injured as INT64)) as PERSONS_INJURED,
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions`
GROUP BY collision_date, NEIGH;
```
 We can then work on the values which know the latitude and longitude.  Fortunatley, google's public dataset also contains a New York bourghs, zone names, bourgh and geography spatial data.  This dataset is called __new_york_taxi_trips &#8594; taxi_zome_geom__

 In order to test this dataset, first I found a record which had a null bourrgh and a longitude and latitude (for brevity I have not shown all the colums here): 
|borough|latitude |longitude|location              |
|null   |40.680088|-73.94398|(40.680088, -73.94398)|

We have our latitude and longitude, so can we find where this is?  
```sql
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` tz_loc
WHERE (ST_DWithin(tz_loc.zone_geom, ST_GeogPoint(-73.94398, 40.680088),0))
```
This retuns zone_id, zone_name, bourgh and a polygon for the zone_geom - but we care only about the bourgh - which in this case is "Brooklyn"

So to further refine this query:
```sql
SELECT UPPER(borough) FROM `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` tz_loc
WHERE (ST_DWithin(tz_loc.zone_geom, ST_GeogPoint(-73.94398, 40.680088),0))
```
We now need to create a new view to see if we can use the above query for our 382,661 records where we have the latitude and location for.

CRAP QUERY
```sql
CREATE VIEW `uhi-assignment-1.assignment.collisions_data_lat_long` AS
SELECT CAST(timestamp as DATE) as collision_date, 
COUNT(CAST(timestamp as DATE)) as NUM_COLLISIONS, 
SELECT CAST(UPPER(borough)as STRING) AS NEIGH FROM `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` tz_loc
WHERE (ST_DWithin(tz_loc.zone_geom, ST_GeogPoint(ds.longitude, ds.latitude),0)),
SUM(CAST(number_of_cyclist_killed as INT64)) as CYCLISTS_KILLED,
SUM(CAST(number_of_cyclist_injured as INT64)) as CYCLISTS_INJURED,
SUM(CAST(number_of_motorist_killed as INT64)) as MOTORISTS_KILLED,
SUM(CAST(number_of_motorist_injured as INT64)) as MOTORISTS_INJURED,
SUM(CAST(number_of_pedestrians_killed as INT64)) as PEDS_KILLED,
SUM(CAST(number_of_pedestrians_injured as INT64)) as PEDS_INJURED,
SUM(CAST(number_of_persons_killed as INT64)) as PERSONS_KILLED,
SUM(CAST(number_of_persons_injured as INT64)) as PERSONS_INJURED,
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` ds
GROUP BY collision_date, NEIGH;
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
