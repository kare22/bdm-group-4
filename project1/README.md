# BigData 2025 Projects Repository

![TartuLogo](./images/logo_ut_0.png)

Project [Big Data](https://courses.cs.ut.ee/2025/bdm/spring/Main/HomePage) is provided by [University of Tartu](https://courses.cs.ut.ee/).

Students: Marielle Lepson, Karel Paan, Andre Ahuna, Aksel Õim

# Project 1: Analyzing New York City Taxi Data

This project analyzes New York City taxi trip data to calculate metrics like taxi utilization, time between trips, and patterns based on boroughs. 
It uses Apache Spark for processing and tools like Sedona and Geopandas for geospatial analysis.

## Data

### Spatial data
Spatial data (`nyc-boroughs.geojson`) is in geojson format, which is a popular way to contain spatial data.

Spatial file contains borough areas of New York as polygons. A polygon is a blob like shape. 
Each area belongs to a specific borough, thus we can compare the pickup and dropoff points and see in which borough they are located in.
```
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "borough": " ... ",
        "geometry": {
          "type": "Polygon",
          "coordinates": [ ... ]
        },
        ...
      },
      ...
    },
    ...
  ]
}
```
The spatial data can be seen on this QGIS map, represented with blue:

![Screenshot 2025-03-09 at 15.16.15.png](images/Screenshot%202025-03-09%20at%2015.16.15.png)

### Trip data
Trip data is originally in csv format, consists of 12 files, total size ~24GB.

Columns (in original order):
- medallion **(necessary)**
  - example value: `B2EBE2244AAE6FD27E9BF259447F03E9`
- hack_license
- vendor_id
- rate_code
- store_and_fwd_flag
- pickup_datetime **(necessary)**
  - example value: `2013-07-12 21:20:21`
- dropoff_datetime **(necessary)**
  - example value: `2013-07-12 21:31:24`
- passenger_count
- trip_time_in_secs
- trip_distance
- pickup_longitude **(necessary)**
  - example value: `-73.974846`
- pickup_latitude **(necessary)**
  - example value: `40.742138`
- dropoff_longitude **(necessary)**
  - example value: `-73.950325`
- dropoff_latitude **(necessary)**
  - example value: `40.786446`

There is a total of 173179759 rows, out of which 173176321 are usable (contain all values for necessary cols). 

## Requirements
### Software, libraries and data files
Software:
- Python
- Apache Spark
- Docker
- Jupyter Notebook

Python Libraries:
- pandas
- geopandas
- sedona
- pyspark
- shapely

Docker:
- Docker Compose

Data Files:  
Sample data from Moodle 
- `nyc-boroughs.geojson`
- `Sample NYC Data.csv`
  - `trip_time_in_secs` and `trip_distance` cols need to be removed from schema
  - date format needs to be changed to `MM-dd-yy HH:mm`

or   

- http://www.andresmh.com/nyctaxitrips/  
- Trips and fares data

Example:

![Screenshot 2025-03-09 at 15.22.54.png](images/Screenshot%202025-03-09%20at%2015.22.54.png)

### Setup

Settings -> Resources must be set like this: 
![Screenshot 2025-03-08 at 20.02.45.png](images/Screenshot%202025-03-08%20at%2020.02.45.png)

* Add data to `data`folder
  - save trip_data folder from http://www.andresmh.com/nyctaxitrips/  (Current notebook version works with this)
  - save  `nyc-boroughs.geojson` and `Sample NYC Data.csv`  
* Run Docker compose file from project directory: 
```bash
docker compose -f compose.yml up -d
```
* Notebooks are automatically hooked to Docker
* Then queries can be run from `notebook.ipynb` file.

Under the hood we are hooking `sedona` and `geopandas` packages to help work with geospatial data.

### License

Licensed under the Apache 2.0 License.

## Queries 
### Data Enrichment
All the queries must be done after data enrichment, which means that to the taxi ride data set was added pick up and drop off borough names.

#### Before optimizing
Before queries, it processes NYC taxi trip data and enriches it with borough-level geospatial information using Spark and Sedona. It starts by initializing a Spark session with Sedona for geospatial processing. Trip data is loaded from CSV files with a predefined schema, selecting only essential columns to optimize performance. NYC borough boundary data is read from a GeoJSON file, converted to WKT format, and transformed into a Spark DataFrame. A helper function converts longitude and latitude into WKT Points, which are then transformed into geometries for pickup and dropoff locations. The trip data is joined with borough boundaries using spatial intersections to determine the pickup and dropoff boroughs. Unnecessary columns are removed, and timestamps are formatted for consistency. Finally, the processed data, including medallion, pickup/dropoff boroughs, and timestamps, is written as a Parquet file for efficient storage and querying.

### Time :
#### Before optimizing
- Preprocessing: 16+ hours or more
- Queries: 1 hour

#### After optimizing
- Set spark.driver.memory and spark.executor.memory to 8g for better performance.
- Saves intermediate results (data.parquet, data_ready.parquet) to avoid reprocessing.
- Skips expensive transformations if processed data already exists.
- Ensures data integrity and avoids slow schema inference with a predefined schema.
- Drops rows with missing critical columns (pickup_longitude, dropoff_longitude, etc.).
- Used .repartition(40) to improve parallel processing:   
```bash
df_geom = df_geom.repartition(40)
df_trip_w_points = df_trip_w_points.repartition(40)
```
- Uses yyyy-MM-d HH:mm:ss format for Spark-compatible timestamp conversion.
- Borough data (df_geom) is repartitioned instead of broadcasted for better scalability.
- Removes unnecessary columns before joins to reduce memory usage. Drop rows where col is NULL. 
- Stores df_final in Parquet format for faster future reads.

Time:
- Preprocessing: 1-2 hours
- Queries: 5 minutes


### Queries explanation
All the queries are done after data enrichment, which means that to the taxi ride data set was added pick up and drop off borough names.

The solution preprocesses the taxi trip data by sorting it by pickup time for each taxi (medallion) and calculating the idle time between consecutive trips. It then filters out invalid idle times (negative or greater than 4 hours). After optimization, the preprocessing uses advanced group-by with window functions.
1) Query 1: Utilization per taxi/driver
  - Description : Compute the fraction of time that a cab is on the road and occupied.
  - Solution : The data is grouped by medallion (taxi), and the average trip time and average idle time are calculated. The utilization is then computed as the ratio of total trip time to the sum of total trip time and idle time.
2) Query 2: Average time for a taxi to find its next fare per destination borough
  - Description: The average time it takes for a taxi to find its next fare(trip) per destination borough. This
can be computed by finding the difference of time, e.g. in seconds, between the dropoff
of a trip and the pickup of the next trip.
  - Solution: The data is grouped by dropoff_borough, and the average idle time (the time between drop-off and the next pickup) is calculated to determine how long taxis wait for their next fare in each borough.
3) Query 3: Number of trips that started and ended within the same borough
  - Description: Count of the number of trips that started and ended within the same borough,
  - Solution: The data is filtered to remove rows where either pickup_borough or dropoff_borough is null. Then, it counts the number of trips where the pickup_borough and dropoff_borough are the same.
4) Query 4: Number of trips that started in one borough and ended in another
  - Description: Count of the number of trips that started in one borough and ended in another one
  - Solution: The data is filtered to remove rows where either pickup_borough or dropoff_borough is null. Then, it counts the number of trips where pickup_borough and dropoff_borough are not the same.

To check that Q3 and Q4 have correct result, under "Miscellaneous" is a check that Q3 and Q4 counts match the total number of trips.
