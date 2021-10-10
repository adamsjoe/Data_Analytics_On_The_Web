# Background

Trying to extract data from a google hosted public dataset.
The data is the number of collisions in New York betwee 2012 and now (ish)

We have this hosted on google
bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions

So first, I created a view using 
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

but as I got into my assignment realised, that all I had done was to group the data so that we recorded numbers of collisions - and we may need to use locations in the assignment.
So I had another look at my data:

```sql
select distinct (borough) FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions`;
```
which yielded the following:
|Row|bourgh       |
|:-: |:-:          |
|1	 |BROOKLYN     |
|2   |BRONX        |
|3	 |QUEENS       |
|4	 |MANHATTAN    |
|5	 |STATEN ISLAND|
|6   |null         |

What I noticed was that the "null" records did not have a borough set in the dataset, but some of them had latitude and longitude set.

So I thought I could change my query to make use of this.

My first attempt was this:
```sql
CREATE VIEW `uhi-assignment-1.assignment.collisions_data_bourgh` AS
SELECT CAST(timestamp as DATE) as collision_date, 
COUNT(CAST(timestamp as DATE)) as NUM_COLLISIONS, 
CASE 
    WHEN ds.borough IS NOT NULL THEN CAST(borough as STRING) -- when the borough is set
    WHEN ((ds.latitude IS NOT NULL or ds.longitude IS NOT NULL) AND ds.borough IS NULL) THEN (SELECT CAST(UPPER(tz_loc.borough)as STRING) FROM `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` tz_loc WHERE (ST_DWithin(tz_loc.zone_geom, ST_GeogPoint(ds.longitude, ds.latitude),0))) -- when the borough is null and either lat or long is not null
    WHEN (ds.latitude IS NULL OR ds.longitude IS NULL OR ds.borough IS NULL) THEN "Unknown"
END AS NEIGHBORHOOD,
SUM(CAST(number_of_cyclist_killed as INT64)) as CYCLISTS_KILLED,
SUM(CAST(number_of_cyclist_injured as INT64)) as CYCLISTS_INJURED,
SUM(CAST(number_of_motorist_killed as INT64)) as MOTORISTS_KILLED,
SUM(CAST(number_of_motorist_injured as INT64)) as MOTORISTS_INJURED,
SUM(CAST(number_of_pedestrians_killed as INT64)) as PEDS_KILLED,
SUM(CAST(number_of_pedestrians_injured as INT64)) as PEDS_INJURED,
SUM(CAST(number_of_persons_killed as INT64)) as PERSONS_KILLED,
SUM(CAST(number_of_persons_injured as INT64)) as PERSONS_INJURED,
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` ds
GROUP BY collision_date, NEIGHBORHOOD;
```
Which gave an error
 **"LEFT OUTER JOIN cannot be used without a condition that is an equality of fields from both sides of the join. "**
 
 A bit of stackoverflow help resulted in the query becoming:
 ```sql
CREATE VIEW `uhi-assignment-1.assignment.collisions_data_bourgh22` AS
SELECT
CAST(timestamp AS DATE) AS collision_date,
COUNT(CAST(timestamp AS DATE)) AS NUM_COLLISIONS,
CASE
  WHEN ds.borough IS NOT NULL THEN CAST(borough AS STRING) -- when the borough is set
  WHEN ds.borough IS NULL AND ds.location IS NOT NULL 
    THEN (
          SELECT CAST(UPPER(tz_loc.borough) as STRING)
                FROM bigquery-public-data.new_york_taxi_trips.taxi_zone_geom tz_loc
                  WHERE 
                      ST_DWithin(tz_loc.zone_geom, 
                          ST_GeogPoint(CAST(ds.longitude AS FLOAT64), 
                              CAST(ds.latitude AS FLOAT64)),0) 
                              AND tz_loc.borough = ds.borough
          ) 
  WHEN (ds.latitude IS NULL AND ds.longitude IS NULL AND ds.borough IS NULL) THEN "Unknown"
END
AS NEIGHBORHOOD,
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
NEIGHBORHOOD 
```
But when I queried the view, I could see unknowns and nulls (lots of nulls) in borough.

I put some concats in to see what was going on 
```sql
CREATE VIEW `uhi-assignment-1.assignment.collisions_data_bourgh22` AS
SELECT
CAST(timestamp AS DATE) AS collision_date,
COUNT(CAST(timestamp AS DATE)) AS NUM_COLLISIONS,
CASE
  WHEN ds.borough IS NOT NULL THEN CONCAT('AA ', CAST(borough AS STRING)) -- when the borough is set
  WHEN ds.borough IS NULL AND ds.location IS NOT NULL 
    THEN (
          SELECT CONCAT('BB ', CAST(UPPER(tz_loc.borough) as STRING))
                FROM bigquery-public-data.new_york_taxi_trips.taxi_zone_geom tz_loc
                  WHERE 
                      ST_DWithin(tz_loc.zone_geom, 
                          ST_GeogPoint(CAST(ds.longitude AS FLOAT64), 
                              CAST(ds.latitude AS FLOAT64)),0) 
                              AND tz_loc.borough = ds.borough
          ) 
  WHEN (ds.latitude IS NULL AND ds.longitude IS NULL AND ds.borough IS NULL) THEN "CC Unknown"
END
AS NEIGHBORHOOD,
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
NEIGHBORHOOD
```
I thought my where second WHEN Clause was wrong.  But I think the issue is on the line
```sql
AND tz_loc.borough = ds.borough
```
As the bourgh will be blank in the ds row.

But I can't figure out how to get around it.


