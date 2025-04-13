# BigData 2025 Projects Repository
![TartuLogo](../static/logo_ut_0.png)

Project [Big Data](https://courses.cs.ut.ee/2025/bdm/spring/Main/HomePage) is provided by [University of Tartu](https://courses.cs.ut.ee/).

Students: Marielle Lepson, Karel Paan, Andre Ahuna, Aksel Õim

# Project 3: Analysing Flight Interconnected Data
The objective of this project is to use Spark’s APIs to analyze the flight interconnected data
to understand the popularity of the airports and flight patterns.

## Data
The 2009.csv data file contains information of U.S. domestic airline flights in 2009. 
Each row represents a single flight and it includes information such as flight date, airline, flight number, origin and destination airports, scheduled and actual times,
delays, cancellation and etc.

The project's data is in cvs file named `2009.csv`. 

The full data file can be obtained via this [link](https://drive.google.com/file/d/1trFtRCe3xPBLr90hIWBF__OqppEnJPR_/view?usp=sharing).

### Flight Interconnected Data 
Columns:

FL_DATE: Flight date, represents the date of the flight  
Example: 2009-01-01

OP_CARRIER: Carrier code, the airline operating the flight  
Example: XE

OP_CARRIER_FL_NUM: Flight number assigned by the carrier  
Example: 1204

ORIGIN: Departure airport code  
Example: DCA

DEST: Arrival airport code  
Example: EWR

CRS_DEP_TIME: Scheduled departure time  
Example: 1100

DEP_TIME: Actual departure time  
Example: 1058.0

DEP_DELAY: Departure delay in minutes. Negative if early  
Example: -2.0

TAXI_OUT: Taxi-out time in minutes (gate to takeoff)  
Example: 18.0

WHEELS_OFF: Time when wheels left the ground  
Example: 1116.0

WHEELS_ON: Time when wheels touched down  
Example: 1158.0

TAXI_IN: Taxi-in time in minutes  
Example: 8.0

CRS_ARR_TIME: Scheduled arrival time  
Example: 1202

ARR_TIME: Actual arrival time  
Example: 1206.0

ARR_DELAY: Arrival delay in minutes. Negative if early  
Example: 4.0

CANCELLED: Whether the flight was canceled (1.0 = yes, 0.0 = no)  
Example: 0.0

CANCELLATION_CODE: Reason for cancellation  
Example: A (Carrier)

DIVERTED: Whether the flight was diverted (1.0 = yes, 0.0 = no)  
Example: 0.0

CRS_ELAPSED_TIME: Scheduled flight duration in minutes  
Example: 62.0

ACTUAL_ELAPSED_TIME: Actual flight duration in minutes  
Example: 68.0

AIR_TIME: Time spent in the air (minutes)  
Example: 42.0

DISTANCE: Flight distance in miles  
Example: 199.0

CARRIER_DELAY: Delay caused by the airline  
Example: null or 15.0

WEATHER_DELAY: Delay caused by weather conditions  
Example: null or 10.0

NAS_DELAY: Delay caused by National Airspace System  
Example: null or 5.0

SECURITY_DELAY: Delay due to security reasons  
Example: null or 0.0

LATE_AIRCRAFT_DELAY: Delay due to a previous flight arriving late  
Example: null or 20.0

Unnamed: 27: Extra column with all values as null

## Requirements

### Software, libraries and data files
Software:
- Docker
- Jupyter Notebook
- Docker Compose

Data Files:  
- A CSV file that consists of flight data, named `2009.csv`. The file size is 802,2 MB.

Data folder view (other files are from other projects, but most important is 2009.csv): 
![folder.png](img/folder.png)

### Setup
- Save csv file to /data location
- Navigate to project3 directory `cd project3`
- Run Docker compose file from project directory: `docker compose up -d`.
- Access the project on http://localhost:8891/lab/tree/notebook.ipynb

## Queries 
### Query 0
Description: Read the csv file and create a graph using Graphframes in the Spark environment.
Solution: 
- In the first cell, the code initialized a PySpark SparkSession with Delta Lake support and includes GraphFrames library for graph processing. It configures memory settings and sets Spark SQL extensions for Delta operations.
- In cell 2, import necessary functions and classes for graph analysis using GraphFrames in PySpark. We import GraphFrame for graph operations and PySpark SQL functions like col, coalesce, lit and sum for dataframe transformations.
- In cell 3, read the CSV file ( 2009.csv) located in the /.data directory into the spark dataframe. The header=True, tells Spark to read the first row as a header.
- In cell 4, create a list of unique airports by selecting origin (ORIGIN) and destination (DEST), columns and renaming them to one common column id, then removing duplicates. These serve as nodes of the graph. Then we define the edges, that represent flights, where each edge goes from source to destination airport. Then we create the GraphFrame graph object using the nodes and edges defined earlier to create flight network. Cache vertices and edges, so that they are stored in memory and don't have to be recomputed again. 
- In cell 5, persist the GraphFrame in memory using gf.persist(), this improves performance for repeated graph operations. The vertices contain a single column named id and edges contain two columns : src and dst.
`GraphFrame(v:[id: string], e:[src: string, dst: string])`
 
Analysis: The CSV file is successfully read into dataframe.
All the necessary functions, classes and Graphframes are imported. A graph object is created, where nodes represent airports (using their codes) and edges represent direct flights between them.

Example of displayed data before creating graph:
![dataframe.png](img/dataframe.png)

### Query 1
Description: Compute different statistics : in-degree, out-degree, total degree and triangle count.
Solution: 
- In cell 6, Use gf.InDegrees to return the expected results for testing purposes. Indegrees returns a dataframe where each row contains an airport (by id) and its in-degree, which is the number of incoming flights. Meaning basically how many times it appeared as a destination. Compute the actual value by grouping the edges in dataframe by destination and counting all occurances. Compare with expected values. 
- In cell 7, Use gf.OutDegrees to return the expected out-degrees counts to view how many flights leave each airport. Then manually compute the out-degrees, by grouping airports on src location and counting all. I compare the expected results with manual results to review the accuracy.
- In cell 8, calculate the total degree for each airport. First do outer join on in_degree and out_degree dataframes using airport id. This means that airports appear only in one. Then use coalesce function to replace any null values. This is important for airports missing either in-degree or out-degree. Then display each airports inDegree, outDegree and totalDegree.
- In cell 9, manually compute the number of triangles each airport is part of. A triangle is a closed loop of three airports where each pair is directly connected by flights. First, build undirected edges to ensure direction doesn’t matter. Then find all valid triangles using join. After that, count how many times each airport appears in a triangle. Finally, compare results with the triangleCount() from GraphFrames to verify if results match.

Analysis:

**NB: I added images here to visualize the results. These tables in the images are NOT sorted, but are included for visual representation of each part.**

**For analysis, I used `orderBy()` with `ascending=False` or `ascending=True` to sort and visualize the highest and lowest counts accurately.**


1) In-degree - 
This table shows the number of incoming flights for each airport. After sorting, the highest in-degree is at ATL with 417 457 incoming flights, followed by ORD (313 769) and DFW (264 398). JFK has 119 571 and BOS has 110 463, also ranking among the busiest. The lowest in-degree is seen at PIR with just 3 flights and others like RHI (41) and BJI (55) show very limited incoming traffic. Example of data output:
![indegree.png](img/indegree.png)
3) Out-degree - 
This table shows the number of outgoing flights per airport. After sorting, the highest out-degree again belongs to ATL with 417 449 flights, followed by ORD (313 848) and DFW (264 396). JFK (119 574) and BOS (110 460) continue to appear among the busiest. On the lower end, PIR (3), RHI (41), and BJI (54) show the lowest out-degree, showing minimal departures. Example of data output:
![outdegree.png](img/outdegree.png)
5) Total degree - 
This table reflects the overall activity (incoming + outgoing) per airport.  After sorting, ATL has the highest total degree of 834 906, making it the most active airport in the dataset. Other high-traffic airports are ORD (627 617) and DFW (528 794). But, PIR has the lowest total degree at 6, followed by RHI (82) and BJI (109), which shows extremely limited overall flight activity. Example of data output:

![totaldegree.png](img/totaldegree.png)
7) Triangle count - 
This table shows how many three-airport roundtrip loops (triangles) each airport is part of, revealing local connectivity and network density. After sorting, The highest triangle count is at ATL (1761), followed by ORD (1678) and DFW (1503). Airports like MEM (1105), JFK (942), and BOS (860) also show strong local connectivity. At the same time, many airports (TEX, GST and EKO) have a triangle count of 0, meaning they are not part of any interconnected clusters. Example of data output:
![trianglecount.png](img/trianglecount.png)


### Query 2
Description: Compute the total number of triangles in the graph.
Solution: 
- In cell 10, calculate the total number of unique triangles in the graph. It sums up the number of triangles each airport is part of and divides the total by 3, since each triangle is counted once for each of its three vertices. The same is done using the triangleCount() method for comparison. Both results are printed to confirm they match.

Analysis: The graph contains a total number of 16 015 unique triangles. This represents a set of three interconnected airports. These triangles show closed flight routine among 3-airports.

### Query 3
Description: Compute a centrality measure of your choice natively on Spark using Graphframes.

Solution: 
In here we chose eigenvector centrality, which measures the influence of an airport in a network based on the importance of its neighbors. 
- In cell 11, convert the directed graph to undirected graph by adding reverse edges. Then initialize all
vertex scores to 1, for a iterative computation.
- In cell 12, perform 10 iterations all together, where each node is updating its score based on the sum of the neighbors scores. After each iteration, scores are normalized to keep values stable.
- In cell 13, display the results sorted by descending order to show the most central airports.
  
Analysis: This analysis shows which airports are the most influencial in the network, based on not connections count but because they have many connections to other very important airports. 

The top airports by eigenvector centrality measure are ATL (0.01876), ORD (0.01810) and DFW (0.01694).
These airports are all very well connected to other highly influential airports. This might be because these airports are at the center of highly connected regions. This confirms that these airports are busy, but also connect to other most influencial airports across country. Other airports are DTW, DEN, MSP and IAH, which also show roles in dense and highly connected areas. These results align well with total degree and triangle count rankings. 
![centrality.png](img/centrality.png)


### Query 4
Description: Implement the PageRank algorithm natively on Spark using Graphframes.

Solution: 
- In cell 14, we apply the pageRank function from GraphFrames, which calculates the importance of each airport based on how many other important airportant point to it. We reset the probability to 0.15 and then run the algorithm for 5 iterations. Then we sort he results in descending order by the pagerank score to find the most influencial airports. 

Analysis: The top airport are ATL (19.51), ORD (13.20) and DFW(12.01) like in other resulst as well. Other big airports like LAX also rank high, confirming their importance in air network. These results match and are consistent with previous results. But this shows that being connected to well-ranked airports is more important than just having many connections. Good example for this is JFK airport, which had total degree of 239 145, but pagerank is 4.57 which is quite low. Then SLC which total degree is 263 368 and pagerank is 7.21. This shows that SLC is more tightly connected to other highly ranked airports than JFK. 

![pagerank.png](img/pagerank.png)

### Query 5
Description: Find the group of the most connected airports

Solution: 
- In cell 15, we compute the degree centrality by summing each airorts in-degree and out-degree. This gives direct flight connections per airport

Analysis: The most connected airports are ATL (834 906), ORD (627 617) and DFW (528 794).
These airports are the most directly connected in the entire network, meaning they are either receiveing or sending out the largest amount of flights. These offer the most direct flight options aswell.  These airports are the major airports or bottlenecks are are crucial for the working air traffix in U.S.
This measure shows how well-connected the airport is overall. Unline other methods before, it does not consider the importance of connected nodes, but rather just the total number of connections. 

![degreecentrality.png](img/degreecentrality.png)

### License
Licensed under the Apache 2.0 License.
