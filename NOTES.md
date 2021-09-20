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
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, mxpsd, gust, max, min, prcp, sndp, fog 
FROM `bigquery-public-data.noaa_gsod.gsod2012` WHERE stn='725060'
union all
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, mxpsd, gust, max, min, prcp, sndp, fog 
FROM `bigquery-public-data.noaa_gsod.gsod2013` WHERE stn='725060'
union all
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, mxpsd, gust, max, min, prcp, sndp, fog 
FROM `bigquery-public-data.noaa_gsod.gsod2014` WHERE stn='725060' AND wban='14756'
union all
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, mxpsd, gust, max, min, prcp, sndp, fog 
FROM `bigquery-public-data.noaa_gsod.gsod2015` WHERE stn='725060'
union all
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, mxpsd, gust, max, min, prcp, sndp, fog 
FROM `bigquery-public-data.noaa_gsod.gsod2016` WHERE stn='725060'
union all
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, mxpsd, gust, max, min, prcp, sndp, fog 
FROM `bigquery-public-data.noaa_gsod.gsod2017` WHERE stn='725060'
union all
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, mxpsd, gust, max, min, prcp, sndp, fog 
FROM `bigquery-public-data.noaa_gsod.gsod2018` WHERE stn='725060'
union all
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, mxpsd, gust, max, min, prcp, sndp, fog 
FROM `bigquery-public-data.noaa_gsod.gsod2019` WHERE stn='725060'
union all
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, mxpsd, gust, max, min, prcp, sndp, fog 
FROM `bigquery-public-data.noaa_gsod.gsod2020` WHERE stn='725060'
union all
SELECT DATE(CAST(year as INT64), CAST(mo as INT64), CAST(da as INT64)) as date, year, mo, da, temp, dewp, slp, visib, mxpsd, gust, max, min, prcp, sndp, fog 
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

2012,2016,2020 are leap years