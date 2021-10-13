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

To begin with, extract the number of collisions per day, I have also chosen to extract injuries as this could be usful later.  In addition to this, I chose to extract the borough the accident happened in.

I wanted to try to find out if I could extract the neigbourhood, so I first found how many boroughs there are in the dataset:
```sql
select distinct (borough) FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions`;
```
which yielded the following:
|Row|bourgh       |
|:-:|:-:          |
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
|:-:|:-:          |:-:    |
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
|:-:|:-:          |:-:    |:-:             |
|1	|BROOKLYN     |397447 |21.7545301982198|
|2  |BRONX        |183043 |10.0189823324185|
|3	|QUEENS       |338237 |18.5136308253812|
|4	|MANHATTAN    |291061 |15.9314205768921|
|5	|STATEN ISLAND|53245  |2.91440106581308|
|6  |null         |563929 |30.8670350012753|

We can see that, as a percentage, Staten Island amounts for just under 3% of all the accidents in our dataset.  At this point, we could decide that with such a small amount of the total occuring here that we could discard this. Howver, the "null" bourghs will most certainly contain accidents which occured in the bourgh of Staten Island (and it could also contain accidents for the other bourghs too.)  So before deciding to discard this, could somehow find a way to determine the bourgh where no bourgh was recorded?
We do have latitude and longitude, but simply running:
```sql
select * from `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` WHERE borough is null
```
shows some latitude and longitude with null values, so let's check this further:
```sql
select * from `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` WHERE borough is null and (latitude is null or longitude is null)
```
This returns 181268 rows - this comes to to approx 9.9% of our records which we have no bourgh, latitude and longitude for ((181268 ÷ 1826962) * 100)

If we look at those which have no borough but a latitude or longitude, 
```sql
select * from `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` WHERE borough is null and (latitude is null or longitude is not null)
```
This returns 382661 rows.  

382661 + 181268 = 563929, which matches our orignal count or null bourghs.  Our data is still correct.

So we have 3 scenarios here to deal with:
1.  The borough is set, we use that.
2.  The borough is empty, but we have latitude and longitude co-ordinates.
3.  The borough is empty, as is the latitude and longitude.

But, the question still remains, can we use the latitude and longitude to?  We can't discard that data, there is far too much of it.  Also, if we discard that data and the 10% which has no location data, we'd be loosing 30%  So we need to find a way to make use of that 20%

Fortunatley, google's public dataset also contains a New York bourghs, zone names, bourgh and geography spatial data.  This dataset is called __new_york_taxi_trips &#8594; taxi_zome_geom__

 n order to test this dataset, first I found a record which had a null bourrgh and a longitude and latitude (for brevity I have not shown all the colums here): 
|borough|...|latitude |longitude|location              |
|:-:    |:-:|:-:      |:-:      |:-:                   |
|null   |...|40.680088|-73.94398|(40.680088, -73.94398)|

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
I have used the UPPER() function to extract the bourgh name to be the same as the ones which are stored in the collisions data.  In this case, the above query yields "BROOKLYN"

And to check this view is correct, we can put those values into google and see that the point is in Brooklyn.  (inset image)

So, what we need to do now is to create our view.  I couldn't get the subquery to work, so have decided to work on this in stages.

```sql
CREATE VIEW `uhi-assignment-1.assignment.collisions_data_bourgh_draft` AS
  SELECT
  CAST(timestamp AS DATE) AS collision_date,
  COUNT(CAST(timestamp AS DATE)) AS NUM_COLLISIONS,
  CASE
    WHEN ds.borough IS NOT NULL THEN CAST(borough AS STRING) -- when the borough is set
    WHEN ((ds.latitude IS NOT NULL or ds.longitude IS NOT NULL) AND ds.borough IS NULL) THEN "BB"
    WHEN (ds.latitude IS NULL OR ds.longitude IS NULL OR ds.borough IS NULL) THEN "Unknown"
END
  AS NEIGHBORHOOD,
  CAST(ds.latitude AS FLOAT64) AS LAT,
  CAST(ds.longitude AS FLOAT64)  AS LONG,
  SUM(CAST(number_of_cyclist_killed AS INT64)) AS CYCLISTS_KILLED,
  SUM(CAST(number_of_cyclist_injured AS INT64)) AS CYCLISTS_INJURED,
  SUM(CAST(number_of_motorist_killed AS INT64)) AS MOTORISTS_KILLED,
  SUM(CAST(number_of_motorist_injured AS INT64)) AS MOTORISTS_INJURED,
  SUM(CAST(number_of_pedestrians_killed AS INT64)) AS PEDS_KILLED,
  SUM(CAST(number_of_pedestrians_injured AS INT64)) AS PEDS_INJURED,
  SUM(CAST(number_of_persons_killed AS INT64)) AS PERSONS_KILLED,
  SUM(CAST(number_of_persons_injured AS INT64)) AS PERSONS_INJURED,
FROM
  bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions ds 
GROUP BY
  collision_date,
  NEIGHBORHOOD,
  LAT, 
  LONG
```

In this query, we create a new view which will, for each date in the dataset, print the sum of the injuries.  Now, the sum will always be truely summed, as we are grouping on the lat and long, but we can modify this later.

In order to check the query, I selected a date and extracted all the records for that date:

```sql
select * from `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` where date(timestamp) = '2021-04-16'
```
I saved this off as a local CSV file and brought this into excel (see file 'data_check_1') I then did a sum on the columns for pedestrian, motorists, etc on killed and injured.  This gave the following:

|Num Records|number_of_cyclist_injured|number_of_cyclist_killed|number_of_motorist_injured|number_of_motorist_killed|number_of_pedestrians_injured|number_of_pedestrians_killed|number_of_persons_injured|number_of_persons_killed|
|:--:       | :--:                    | :--:                   | :--:                     | :--:                    | :--:                        | :--:                       | :--:                    | :--:                   |
|298        |11                	      |0                       |99                        |0                        |16                           |0                           |127                      |0                       |

I then ran the following query which would extract all columns from the newly created view for the same date:

```sql
SELECT 
 SUM(CYCLISTS_INJURED) AS CYCLISTS_INJURED,
 SUM(CYCLISTS_KILLED)  AS CYCLISTS_KILLED,
 SUM(MOTORISTS_INJURED)  AS MOTORISTS_INJURED,
 SUM(MOTORISTS_KILLED)  AS MOTORISTS_KILLED,
 SUM(PEDS_INJURED)  AS PEDS_INJURED,
 SUM(PEDS_KILLED)  AS PEDS_KILLED,
 SUM(PERSONS_INJURED)  AS PERSONS_INJURED,
 SUM(PERSONS_KILLED)  AS PERSONS_KILLED,
FROM `uhi-assignment-1.assignment.collisions_data_bourgh_draft` 
WHERE collision_date = '2021-04-16'
```

|Row|collision_date|NUM_COLLISIONS|CYCLISTS_KILLED|CYCLISTS_INJURED|MOTORISTS_KILLED|MOTORISTS_INJURED|PEDS_KILLED|PEDS_INJURED|PERSONS_KILLED|PERSONS_INJURED|
|:-:|:-:           |:-:           |:-:            |:-:             |:-:             |:-:              | :-:       | :-:        | :-:          | :-:           |
|1  |2021-04-16    |277           |0              |11              |0               |101              |0          |16          |0             |129            |

The values match - the number of records are different.  But that aside, we are looking good.  Our "draft" view is holding up.

## Now to add in a day field

```sql
CREATE VIEW `uhi-assignment-1.assignment.collisions_data_final` 
AS SELECT FORMAT_DATE("%u", collision_date) as day, collision_date, NEIGHBORHOOD, LAT, LONG, NUM_COLLISIONS, CYCLISTS_KILLED, CYCLISTS_INJURED, MOTORISTS_KILLED, MOTORISTS_INJURED, PEDS_KILLED, PEDS_INJURED, PERSONS_KILLED, PERSONS_INJURED
FROM `uhi-assignment-1.assignment.collisions_data_bourgh_draft`;
```

Generates a new view. 

To check the day has been added correctly, 

```sql
SELECT * FROM `uhi-assignment-1.assignment.collisions_data_final`  where collision_date = '2019-12-04'
```
Which shows that the 4th December, 2019 is a Wednesday (value 3) (insert image)

# Finishing touches

Now we need to collate the collisions data with the weather data.

```sql
CREATE TABLE `uhi-assignment-1.assignment.collated_data` 
AS SELECT day, year, mo, da, collision_date,  NEIGHBORHOOD, LAT, LONG,temp, dewp, slp, visib, wdsp, mxpsd, gust, max, min, prcp, sndp, fog, CYCLISTS_KILLED, CYCLISTS_INJURED, MOTORISTS_KILLED, MOTORISTS_INJURED, PEDS_KILLED, PEDS_INJURED,PERSONS_KILLED, PERSONS_INJURED, NUM_COLLISIONS FROM `uhi-assignment-1.assignment.weather_2012_to_2021` as weather,
`uhi-assignment-1.assignment.collisions_data_final` as complaints WHERE complaints.collision_date = weather.date
```

Data has been stored in "Final_Data" directory
