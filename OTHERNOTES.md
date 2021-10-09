I had an idea last night - I thought I could do all this in the one big query like this:

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

However when I try to query this view I get "LEFT OUTER JOIN cannot be used without a condition that is an equality of fields from both sides of the join. "

# Trying to split this apart
handle where the borough exists
```sql
CREATE VIEW `uhi-assignment-1.assignment.collisions_data_bourgh` AS
SELECT CAST(timestamp as DATE) as collision_date, 
COUNT(CAST(timestamp as DATE)) as NUM_COLLISIONS, 
CAST(borough as STRING) AS NEIGHBORHOOD, -- when the borough is set
SUM(CAST(number_of_cyclist_killed as INT64)) as CYCLISTS_KILLED,
SUM(CAST(number_of_cyclist_injured as INT64)) as CYCLISTS_INJURED,
SUM(CAST(number_of_motorist_killed as INT64)) as MOTORISTS_KILLED,
SUM(CAST(number_of_motorist_injured as INT64)) as MOTORISTS_INJURED,
SUM(CAST(number_of_pedestrians_killed as INT64)) as PEDS_KILLED,
SUM(CAST(number_of_pedestrians_injured as INT64)) as PEDS_INJURED,
SUM(CAST(number_of_persons_killed as INT64)) as PERSONS_KILLED,
SUM(CAST(number_of_persons_injured as INT64)) as PERSONS_INJURED,
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` ds
WHERE ds.borough IS NOT NULL
GROUP BY collision_date, NEIGHBORHOOD;
```
handle where there are nothing we can use
```sql
CREATE VIEW `uhi-assignment-1.assignment.collisions_data_bourgh_no_lat_long` AS
SELECT CAST(timestamp as DATE) as collision_date, 
COUNT(CAST(timestamp as DATE)) as NUM_COLLISIONS, 
"UNKNOWN" AS NEIGHBORHOOD,
SUM(CAST(number_of_cyclist_killed as INT64)) as CYCLISTS_KILLED,
SUM(CAST(number_of_cyclist_injured as INT64)) as CYCLISTS_INJURED,
SUM(CAST(number_of_motorist_killed as INT64)) as MOTORISTS_KILLED,
SUM(CAST(number_of_motorist_injured as INT64)) as MOTORISTS_INJURED,
SUM(CAST(number_of_pedestrians_killed as INT64)) as PEDS_KILLED,
SUM(CAST(number_of_pedestrians_injured as INT64)) as PEDS_INJURED,
SUM(CAST(number_of_persons_killed as INT64)) as PERSONS_KILLED,
SUM(CAST(number_of_persons_injured as INT64)) as PERSONS_INJURED,
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` ds
CROSS JOIN `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` tz_loc
WHERE ((ds.latitude IS NULL or ds.longitude IS NULL) AND ds.borough IS NULL)
GROUP BY collision_date, NEIGHBORHOOD;
```

now to handle the ones where we have to query things



```sql
CREATE VIEW `uhi-assignment-1.assignment.collisions_data_bourgh22` AS
  SELECT
  CAST(timestamp AS DATE) AS collision_date,
  COUNT(CAST(timestamp AS DATE)) AS NUM_COLLISIONS,
  CASE
    WHEN ds.borough IS NOT NULL THEN CAST(borough AS STRING) -- when the borough is set
    --WHEN (ds.borough IS NULL AND (ds.latitude IS NOT NULL or ds.longitude IS NOT NULL) )
    WHEN (ds.borough IS NULL AND ds.location IS NOT NULL )
      THEN (
            SELECT CONCAT('XX ', CAST(UPPER(tz_loc.borough) as STRING))
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
