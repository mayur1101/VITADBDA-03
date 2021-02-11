# *Analysis of New York Yellow Taxi Trip Data*

## Introduction of NYC Yellow Taxi: 
- ### Yellow Taxis are the only vehicles licensed to pick up street-hailing passengers anywhere in NYC. 
- ### Yellow Taxis charge standard [metered fares](https://www1.nyc.gov/site/tlc/passengers/taxi-fare.page) for all street hail trips.
- ### Yellow Taxi smartphone apps can offer set, upfront pricing for trips booked through an app.
- ### Yellow Taxis are easily identified by their yellow color, taxi “T” markings, and license numbers on the roof and sides of the vehicle.

## Introduction of NYC Yellow Taxi Trip Data:
Variable Name | Description |
--------------|------------------|
**vendorid :**|A code indicating the [TPEP provider](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) that provided the record.<br>
**pickup_datetime:**| The date and time when the meter was engaged.<br>
**dropoff_datetime:**          |  The date and time when the meter was disengaged.<br>
**passanger_count:**        | The number of passengers in the vehicle. This is a driver-entered value.<br>
**trip_distance :**          | The elapsed trip distance in miles reported by the taximeter.<br>
**PULocationID :**| Taxi Zone in which the taximeter was engaged.<br>
**DOLocationID :** |Taxi Zone in which the taximeter was disengaged.<br>
**ratecodeid :**             |The final [rate code](https://www1.nyc.gov/site/tlc/passengers/taxi-fare.page) in effect at the end of the trip.<br>
**store_fwd_flg :**           |This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka “store and forward”, because the vehicle did not have a connection to the server.<br>
**pay_type :**                |Signifying how the passenger paid for the trip.<br>
**fare_amount :**            | The time-and-distance fare calculated by the meter.<br>
**surcharge :**              | $0.30 improvement surcharge,  $0.50 overnight surcharge 8pm to 6am, New York State Congestion Surcharge of $2.50.<br>
**mta_tax :**                | $0.50 MTA tax that is automatically triggered based on the metered rate in use.<br>
**tip_amount :**             | This field is automatically populated for credit card tips. Cash tips are not included.<br>
**toll_amount :**            | Total amount of all tolls paid in trip.<br>
**total_amount :**           | The total amount charged to passengers. Does not include cash tips.





## Objective:
- **The core objective of this project is to analyse the factors for demand for taxis, to find the most pickups, drop-offs of public based on their location, time of most traffic and how to overcome the needs of the public.**

## Architecture Diagram:<br>
<img src="/Final project/Images/arch_dig.png" width=800>

## Code to Get Raw Data from NYC website and store data in S3 bucket:
[Dataingestion](https://github.com/bhavesh677/007/blob/Bhavesh-007/Final%20Project%2023/Code/dataingestion.sh)
```shell
#To download files from internet.
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-01.csv

#Hadoop command to make directory.
hdfs dfs -mkdir rawdata

#Hadoop command to put data from local to hdfs directory. 
hdfs dfs -put yellow_tripdata_2014-01.csv /user/hadoop/rawdata/

#Copy data from hdfs to AWS S3 storage.
hdfs dfs -cp /user/hadoop/rawdata/yellow_tripdata_2014-01.csv s3a://nycproject23/rawdatas3/

#Remove the file from Hdfs and local to free space.
hdfs dfs -rm /user/hadoop/rawdata/yellow_tripdata_2014-01.csv
rm yellow_tripdata_2014-01.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-02.csv
hdfs dfs -put yellow_tripdata_2014-02.csv /user/hadoop/rawdata/
hdfs dfs -cp /user/hadoop/rawdata/yellow_tripdata_2014-02.csv s3a://nycproject23/rawdatas3/
hdfs dfs -rm /user/hadoop/rawdata/yellow_tripdata_2014-02.csv
rm yellow_tripdata_2014-02.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-03.csv
hdfs dfs -put yellow_tripdata_2014-03.csv /user/hadoop/rawdata/
hdfs dfs -cp /user/hadoop/rawdata/yellow_tripdata_2014-03.csv s3a://nycproject23/rawdatas3/
hdfs dfs -rm /user/hadoop/rawdata/yellow_tripdata_2014-03.csv
rm yellow_tripdata_2014-03.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-04.csv
hdfs dfs -put yellow_tripdata_2014-04.csv /user/hadoop/rawdata/
hdfs dfs -cp /user/hadoop/rawdata/yellow_tripdata_2014-04.csv s3a://nycproject23/rawdatas3/
hdfs dfs -rm /user/hadoop/rawdata/yellow_tripdata_2014-04.csv
rm yellow_tripdata_2014-04.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-05.csv
hdfs dfs -put yellow_tripdata_2014-05.csv /user/hadoop/rawdata/
hdfs dfs -cp /user/hadoop/rawdata/yellow_tripdata_2014-05.csv s3a://nycproject23/rawdatas3/
hdfs dfs -rm /user/hadoop/rawdata/yellow_tripdata_2014-05.csv
rm yellow_tripdata_2014-05.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-06.csv
hdfs dfs -put yellow_tripdata_2014-06.csv /user/hadoop/rawdata/
hdfs dfs -cp /user/hadoop/rawdata/yellow_tripdata_2014-06.csv s3a://nycproject23/rawdatas3/
hdfs dfs -rm /user/hadoop/rawdata/yellow_tripdata_2014-06.csv
rm yellow_tripdata_2014-06.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-07.csv
hdfs dfs -put yellow_tripdata_2014-07.csv /user/hadoop/rawdata/
hdfs dfs -cp /user/hadoop/rawdata/yellow_tripdata_2014-07.csv s3a://nycproject23/rawdatas3/
hdfs dfs -rm /user/hadoop/rawdata/yellow_tripdata_2014-07.csv
rm yellow_tripdata_2014-07.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-08.csv
hdfs dfs -put yellow_tripdata_2014-08.csv /user/hadoop/rawdata/
hdfs dfs -cp /user/hadoop/rawdata/yellow_tripdata_2014-08.csv s3a://nycproject23/rawdatas3/
hdfs dfs -rm /user/hadoop/rawdata/yellow_tripdata_2014-08.csv
rm yellow_tripdata_2014-08.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-09.csv
hdfs dfs -put yellow_tripdata_2014-09.csv /user/hadoop/rawdata/
hdfs dfs -cp /user/hadoop/rawdata/yellow_tripdata_2014-09.csv s3a://nycproject23/rawdatas3/
hdfs dfs -rm /user/hadoop/rawdata/yellow_tripdata_2014-09.csv
rm yellow_tripdata_2014-09.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-10.csv
hdfs dfs -put yellow_tripdata_2014-10.csv /user/hadoop/rawdata/
hdfs dfs -cp /user/hadoop/rawdata/yellow_tripdata_2014-10.csv s3a://nycproject23/rawdatas3/
hdfs dfs -rm /user/hadoop/rawdata/yellow_tripdata_2014-10.csv
rm yellow_tripdata_2014-10.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-11.csv
hdfs dfs -put yellow_tripdata_2014-11.csv /user/hadoop/rawdata/
hdfs dfs -cp /user/hadoop/rawdata/yellow_tripdata_2014-11.csv s3a://nycproject23/rawdatas3/
hdfs dfs -rm /user/hadoop/rawdata/yellow_tripdata_2014-11.csv
rm yellow_tripdata_2014-11.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-12.csv
hdfs dfs -put yellow_tripdata_2014-12.csv /user/hadoop/rawdata/
hdfs dfs -cp /user/hadoop/rawdata/yellow_tripdata_2014-12.csv s3a://nycproject23/rawdatas3/
rm yellow_tripdata_2014-12.csv
```

## Code to Read Raw Data from S3 bucket and create Dataframe in PySpark, Perform Cleaning and Transformations, and Create Table in Hive:
[PythonScript](https://github.com/bhavesh677/007/blob/Bhavesh-007/Final%20Project%2023/Code/finalScript.py)
```python
import pyspark
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession

#To create spark Context(The entry point into all functionality in Spark SQL is the SQLContext class)
sc = SparkContext()

#To create sql Context on spark context.(sql context enables application run sql queries programmitically 
#while running sql functions).
sqlContext = SQLContext(sc)

#To create spark session
#appName:-sets a name for application which will be shown in Spark web UI
#config:-Sets a config options.
#master:-Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]" to run locally with 4 cores, 
#or "spark://master:7077" to run on a Spark standalone cluster,"local[*]" to Run Spark locally 
#with as many worker threads as logical cores on your machine.
#getOrCreate:-Gets an existing SparkSession or, if there is no existing one, creates a new one based on the options set in this builder.
spark = SparkSession.builder.master("local").appName("app name").config("spark.some.config.option", 'true').getOrCreate()

#Read csv file from S3 bucket to create Spark dataframe.
sampledf1 = spark.read.option("header",True).csv("s3a://nycproject23/rawdata/yellow_tripdata_2014-01.csv")

#Sampling data
#list of built-in functions available for dataframe
import pyspark.sql.functions as F
#Randomly sample 50% of the data without replacement
sampledf1 = sampledf1.sample(False, 0.50, seed=0)
#To remove white spaces from left and right of the column names.
sampledf1 = sampledf1.withColumnRenamed(' passenger_count',"passenger_count")
sampledf1 = sampledf1.withColumnRenamed(' trip_distance',"trip_distance")
sampledf1 = sampledf1.withColumnRenamed(' pickup_longitude',"pickup_longitude")
sampledf1 = sampledf1.withColumnRenamed(' pickup_latitude',"pickup_latitude")
sampledf1 = sampledf1.withColumnRenamed(' rate_code',"rate_code")
sampledf1 = sampledf1.withColumnRenamed(' store_and_fwd_flag',"store_and_fwd_flag")
sampledf1 = sampledf1.withColumnRenamed(' dropoff_longitude',"dropoff_longitude")
sampledf1 = sampledf1.withColumnRenamed(' dropoff_latitude',"dropoff_latitude")
sampledf1 = sampledf1.withColumnRenamed(' payment_type',"payment_type")
sampledf1 = sampledf1.withColumnRenamed(' fare_amount',"fare_amount")
sampledf1 = sampledf1.withColumnRenamed(' surcharge',"surcharge")
sampledf1 = sampledf1.withColumnRenamed(' mta_tax',"mta_tax")
sampledf1 = sampledf1.withColumnRenamed(' tip_amount',"tip_amount")
sampledf1 = sampledf1.withColumnRenamed(' tolls_amount',"tolls_amount")
sampledf1 = sampledf1.withColumnRenamed(' total_amount',"total_amount")
sampledf1 = sampledf1.withColumnRenamed(' pickup_datetime',"pickup_datetime")
sampledf1 = sampledf1.withColumnRenamed(' dropoff_datetime',"dropoff_datetime")
#Write dataframe into S3 in parquet format.
sampledf1.write.parquet("s3a://nycproject23/sampledrawdata/df1.parquet",mode="overwrite")


sampledf2 = spark.read.option("header",True).csv("s3a://nycproject23/rawdata/yellow_tripdata_2014-02.csv")
import pyspark.sql.functions as F
sampledf2 = sampledf2.sample(False, 0.50, seed=0)
sampledf2 = sampledf2.withColumnRenamed(' passenger_count',"passenger_count")
sampledf2 = sampledf2.withColumnRenamed(' trip_distance',"trip_distance")
sampledf2 = sampledf2.withColumnRenamed(' pickup_longitude',"pickup_longitude")
sampledf2 = sampledf2.withColumnRenamed(' pickup_latitude',"pickup_latitude")
sampledf2 = sampledf2.withColumnRenamed(' rate_code',"rate_code")
sampledf2 = sampledf2.withColumnRenamed(' store_and_fwd_flag',"store_and_fwd_flag")
sampledf2 = sampledf2.withColumnRenamed(' dropoff_longitude',"dropoff_longitude")
sampledf2 = sampledf2.withColumnRenamed(' dropoff_latitude',"dropoff_latitude")
sampledf2 = sampledf2.withColumnRenamed(' payment_type',"payment_type")
sampledf2 = sampledf2.withColumnRenamed(' fare_amount',"fare_amount")
sampledf2 = sampledf2.withColumnRenamed(' surcharge',"surcharge")
sampledf2 = sampledf2.withColumnRenamed(' mta_tax',"mta_tax")
sampledf2 = sampledf2.withColumnRenamed(' tip_amount',"tip_amount")
sampledf2 = sampledf2.withColumnRenamed(' tolls_amount',"tolls_amount")
sampledf2 = sampledf2.withColumnRenamed(' total_amount',"total_amount")
sampledf2 = sampledf2.withColumnRenamed(' pickup_datetime',"pickup_datetime")
sampledf2 = sampledf2.withColumnRenamed(' dropoff_datetime',"dropoff_datetime")
sampledf2.write.parquet("s3a://nycproject23/sampledrawdata/df2.parquet",mode="overwrite")



sampledf3 = spark.read.option("header",True).csv("s3a://nycproject23/rawdata/yellow_tripdata_2014-03.csv")
import pyspark.sql.functions as F
sampledf3 = sampledf3.sample(False, 0.50, seed=0)
sampledf3 = sampledf3.withColumnRenamed(' passenger_count',"passenger_count")
sampledf3 = sampledf3.withColumnRenamed(' trip_distance',"trip_distance")
sampledf3 = sampledf3.withColumnRenamed(' pickup_longitude',"pickup_longitude")
sampledf3 = sampledf3.withColumnRenamed(' pickup_latitude',"pickup_latitude")
sampledf3 = sampledf3.withColumnRenamed(' rate_code',"rate_code")
sampledf3 = sampledf3.withColumnRenamed(' store_and_fwd_flag',"store_and_fwd_flag")
sampledf3 = sampledf3.withColumnRenamed(' dropoff_longitude',"dropoff_longitude")
sampledf3 = sampledf3.withColumnRenamed(' dropoff_latitude',"dropoff_latitude")
sampledf3 = sampledf3.withColumnRenamed(' payment_type',"payment_type")
sampledf3 = sampledf3.withColumnRenamed(' fare_amount',"fare_amount")
sampledf3 = sampledf3.withColumnRenamed(' surcharge',"surcharge")
sampledf3 = sampledf3.withColumnRenamed(' mta_tax',"mta_tax")
sampledf3 = sampledf3.withColumnRenamed(' tip_amount',"tip_amount")
sampledf3 = sampledf3.withColumnRenamed(' tolls_amount',"tolls_amount")
sampledf3 = sampledf3.withColumnRenamed(' total_amount',"total_amount")
sampledf3 = sampledf3.withColumnRenamed(' pickup_datetime',"pickup_datetime")
sampledf3 = sampledf3.withColumnRenamed(' dropoff_datetime',"dropoff_datetime")
sampledf3.write.parquet("s3a://nycproject23/sampledrawdata/df3.parquet",mode="overwrite")


sampledf4 = spark.read.option("header",True).csv("s3a://nycproject23/rawdata/yellow_tripdata_2014-04.csv")
import pyspark.sql.functions as F
sampledf4 = sampledf4.sample(False, 0.50, seed=0)
sampledf4 = sampledf4.withColumnRenamed(' passenger_count',"passenger_count")
sampledf4 = sampledf4.withColumnRenamed(' trip_distance',"trip_distance")
sampledf4 = sampledf4.withColumnRenamed(' pickup_longitude',"pickup_longitude")
sampledf4 = sampledf4.withColumnRenamed(' pickup_latitude',"pickup_latitude")
sampledf4 = sampledf4.withColumnRenamed(' rate_code',"rate_code")
sampledf4 = sampledf4.withColumnRenamed(' store_and_fwd_flag',"store_and_fwd_flag")
sampledf4 = sampledf4.withColumnRenamed(' dropoff_longitude',"dropoff_longitude")
sampledf4 = sampledf4.withColumnRenamed(' dropoff_latitude',"dropoff_latitude")
sampledf4 = sampledf4.withColumnRenamed(' payment_type',"payment_type")
sampledf4 = sampledf4.withColumnRenamed(' fare_amount',"fare_amount")
sampledf4 = sampledf4.withColumnRenamed(' surcharge',"surcharge")
sampledf4 = sampledf4.withColumnRenamed(' mta_tax',"mta_tax")
sampledf4 = sampledf4.withColumnRenamed(' tip_amount',"tip_amount")
sampledf4 = sampledf4.withColumnRenamed(' tolls_amount',"tolls_amount")
sampledf4 = sampledf4.withColumnRenamed(' total_amount',"total_amount")
sampledf4 = sampledf4.withColumnRenamed(' pickup_datetime',"pickup_datetime")
sampledf4 = sampledf4.withColumnRenamed(' dropoff_datetime',"dropoff_datetime")
sampledf4.write.parquet("s3a://nycproject23/sampledrawdata/df4.parquet",mode="overwrite")


sampledf5 = spark.read.option("header",True).csv("s3a://nycproject23/rawdata/yellow_tripdata_2014-05.csv")
import pyspark.sql.functions as F
sampledf5 = sampledf5.sample(False, 0.50, seed=0)
sampledf5 = sampledf5.withColumnRenamed(' passenger_count',"passenger_count")
sampledf5 = sampledf5.withColumnRenamed(' trip_distance',"trip_distance")
sampledf5 = sampledf5.withColumnRenamed(' pickup_longitude',"pickup_longitude")
sampledf5 = sampledf5.withColumnRenamed(' pickup_latitude',"pickup_latitude")
sampledf5 = sampledf5.withColumnRenamed(' rate_code',"rate_code")
sampledf5 = sampledf5.withColumnRenamed(' store_and_fwd_flag',"store_and_fwd_flag")
sampledf5 = sampledf5.withColumnRenamed(' dropoff_longitude',"dropoff_longitude")
sampledf5 = sampledf5.withColumnRenamed(' dropoff_latitude',"dropoff_latitude")
sampledf5 = sampledf5.withColumnRenamed(' payment_type',"payment_type")
sampledf5 = sampledf5.withColumnRenamed(' fare_amount',"fare_amount")
sampledf5 = sampledf5.withColumnRenamed(' surcharge',"surcharge")
sampledf5 = sampledf5.withColumnRenamed(' mta_tax',"mta_tax")
sampledf5 = sampledf5.withColumnRenamed(' tip_amount',"tip_amount")
sampledf5 = sampledf5.withColumnRenamed(' tolls_amount',"tolls_amount")
sampledf5 = sampledf5.withColumnRenamed(' total_amount',"total_amount")
sampledf5 = sampledf5.withColumnRenamed(' pickup_datetime',"pickup_datetime")
sampledf5 = sampledf5.withColumnRenamed(' dropoff_datetime',"dropoff_datetime")
sampledf5.write.parquet("s3a://nycproject23/sampledrawdata/df5.parquet",mode="overwrite")


sampledf6 = spark.read.option("header",True).csv("s3a://nycproject23/rawdata/yellow_tripdata_2014-06.csv")
import pyspark.sql.functions as F
sampledf6 = sampledf6.sample(False, 0.50, seed=0)
sampledf6 = sampledf6.withColumnRenamed(' passenger_count',"passenger_count")
sampledf6 = sampledf6.withColumnRenamed(' trip_distance',"trip_distance")
sampledf6 = sampledf6.withColumnRenamed(' pickup_longitude',"pickup_longitude")
sampledf6 = sampledf6.withColumnRenamed(' pickup_latitude',"pickup_latitude")
sampledf6 = sampledf6.withColumnRenamed(' rate_code',"rate_code")
sampledf6 = sampledf6.withColumnRenamed(' store_and_fwd_flag',"store_and_fwd_flag")
sampledf6 = sampledf6.withColumnRenamed(' dropoff_longitude',"dropoff_longitude")
sampledf6 = sampledf6.withColumnRenamed(' dropoff_latitude',"dropoff_latitude")
sampledf6 = sampledf6.withColumnRenamed(' payment_type',"payment_type")
sampledf6 = sampledf6.withColumnRenamed(' fare_amount',"fare_amount")
sampledf6 = sampledf6.withColumnRenamed(' surcharge',"surcharge")
sampledf6 = sampledf6.withColumnRenamed(' mta_tax',"mta_tax")
sampledf6 = sampledf6.withColumnRenamed(' tip_amount',"tip_amount")
sampledf6 = sampledf6.withColumnRenamed(' tolls_amount',"tolls_amount")
sampledf6 = sampledf6.withColumnRenamed(' total_amount',"total_amount")
sampledf6 = sampledf6.withColumnRenamed(' pickup_datetime',"pickup_datetime")
sampledf6 = sampledf6.withColumnRenamed(' dropoff_datetime',"dropoff_datetime")
sampledf6.write.parquet("s3a://nycproject23/sampledrawdata/df6.parquet",mode="overwrite")

sampledf7 = spark.read.option("header",True).csv("s3a://nycproject23/rawdata/yellow_tripdata_2014-07.csv")
import pyspark.sql.functions as F
sampledf7 = sampledf7.sample(False, 0.50, seed=0)
sampledf7 = sampledf7.withColumnRenamed(' passenger_count',"passenger_count")
sampledf7 = sampledf7.withColumnRenamed(' trip_distance',"trip_distance")
sampledf7 = sampledf7.withColumnRenamed(' pickup_longitude',"pickup_longitude")
sampledf7 = sampledf7.withColumnRenamed(' pickup_latitude',"pickup_latitude")
sampledf7 = sampledf7.withColumnRenamed(' rate_code',"rate_code")
sampledf7 = sampledf7.withColumnRenamed(' store_and_fwd_flag',"store_and_fwd_flag")
sampledf7 = sampledf7.withColumnRenamed(' dropoff_longitude',"dropoff_longitude")
sampledf7 = sampledf7.withColumnRenamed(' dropoff_latitude',"dropoff_latitude")
sampledf7 = sampledf7.withColumnRenamed(' payment_type',"payment_type")
sampledf7 = sampledf7.withColumnRenamed(' fare_amount',"fare_amount")
sampledf7 = sampledf7.withColumnRenamed(' surcharge',"surcharge")
sampledf7 = sampledf7.withColumnRenamed(' mta_tax',"mta_tax")
sampledf7 = sampledf7.withColumnRenamed(' tip_amount',"tip_amount")
sampledf7 = sampledf7.withColumnRenamed(' tolls_amount',"tolls_amount")
sampledf7 = sampledf7.withColumnRenamed(' total_amount',"total_amount")
sampledf7 = sampledf7.withColumnRenamed(' pickup_datetime',"pickup_datetime")
sampledf7 = sampledf7.withColumnRenamed(' dropoff_datetime',"dropoff_datetime")
sampledf7.write.parquet("s3a://nycproject23/sampledrawdata/df7.parquet",mode="overwrite")


sampledf8 = spark.read.option("header",True).csv("s3a://nycproject23/rawdata/yellow_tripdata_2014-08.csv")
import pyspark.sql.functions as F
sampledf8 = sampledf8.sample(False, 0.50, seed=0)
sampledf8 = sampledf8.withColumnRenamed(' passenger_count',"passenger_count")
sampledf8 = sampledf8.withColumnRenamed(' trip_distance',"trip_distance")
sampledf8 = sampledf8.withColumnRenamed(' pickup_longitude',"pickup_longitude")
sampledf8 = sampledf8.withColumnRenamed(' pickup_latitude',"pickup_latitude")
sampledf8 = sampledf8.withColumnRenamed(' rate_code',"rate_code")
sampledf8 = sampledf8.withColumnRenamed(' store_and_fwd_flag',"store_and_fwd_flag")
sampledf8 = sampledf8.withColumnRenamed(' dropoff_longitude',"dropoff_longitude")
sampledf8 = sampledf8.withColumnRenamed(' dropoff_latitude',"dropoff_latitude")
sampledf8 = sampledf8.withColumnRenamed(' payment_type',"payment_type")
sampledf8 = sampledf8.withColumnRenamed(' fare_amount',"fare_amount")
sampledf8 = sampledf8.withColumnRenamed(' surcharge',"surcharge")
sampledf8 = sampledf8.withColumnRenamed(' mta_tax',"mta_tax")
sampledf8 = sampledf8.withColumnRenamed(' tip_amount',"tip_amount")
sampledf8 = sampledf8.withColumnRenamed(' tolls_amount',"tolls_amount")
sampledf8 = sampledf8.withColumnRenamed(' total_amount',"total_amount")
sampledf8 = sampledf8.withColumnRenamed(' pickup_datetime',"pickup_datetime")
sampledf8 = sampledf8.withColumnRenamed(' dropoff_datetime',"dropoff_datetime")
sampledf8.write.parquet("s3a://nycproject23/sampledrawdata/df8.parquet",mode="overwrite")

sampledf9 = spark.read.option("header",True).csv("s3a://nycproject23/rawdata/yellow_tripdata_2014-09.csv")
import pyspark.sql.functions as F
sampledf9 = sampledf9.sample(False, 0.50, seed=0)
sampledf9 = sampledf9.withColumnRenamed(' passenger_count',"passenger_count")
sampledf9 = sampledf9.withColumnRenamed(' trip_distance',"trip_distance")
sampledf9 = sampledf9.withColumnRenamed(' pickup_longitude',"pickup_longitude")
sampledf9 = sampledf9.withColumnRenamed(' pickup_latitude',"pickup_latitude")
sampledf9 = sampledf9.withColumnRenamed(' rate_code',"rate_code")
sampledf9 = sampledf9.withColumnRenamed(' store_and_fwd_flag',"store_and_fwd_flag")
sampledf9 = sampledf9.withColumnRenamed(' dropoff_longitude',"dropoff_longitude")
sampledf9 = sampledf9.withColumnRenamed(' dropoff_latitude',"dropoff_latitude")
sampledf9 = sampledf9.withColumnRenamed(' payment_type',"payment_type")
sampledf9 = sampledf9.withColumnRenamed(' fare_amount',"fare_amount")
sampledf9 = sampledf9.withColumnRenamed(' surcharge',"surcharge")
sampledf9 = sampledf9.withColumnRenamed(' mta_tax',"mta_tax")
sampledf9 = sampledf9.withColumnRenamed(' tip_amount',"tip_amount")
sampledf9 = sampledf9.withColumnRenamed(' tolls_amount',"tolls_amount")
sampledf9 = sampledf9.withColumnRenamed(' total_amount',"total_amount")
sampledf9 = sampledf9.withColumnRenamed(' pickup_datetime',"pickup_datetime")
sampledf9 = sampledf9.withColumnRenamed(' dropoff_datetime',"dropoff_datetime")
sampledf9.write.parquet("s3a://nycproject23/sampledrawdata/df9.parquet",mode="overwrite")


sampledf10 = spark.read.option("header",True).csv("s3a://nycproject23/rawdata/yellow_tripdata_2014-10.csv")
import pyspark.sql.functions as F
sampledf10 = sampledf10.sample(False, 0.50, seed=0)
sampledf10 = sampledf10.withColumnRenamed(' passenger_count',"passenger_count")
sampledf10 = sampledf10.withColumnRenamed(' trip_distance',"trip_distance")
sampledf10 = sampledf10.withColumnRenamed(' pickup_longitude',"pickup_longitude")
sampledf10 = sampledf10.withColumnRenamed(' pickup_latitude',"pickup_latitude")
sampledf10 = sampledf10.withColumnRenamed(' rate_code',"rate_code")
sampledf10 = sampledf10.withColumnRenamed(' store_and_fwd_flag',"store_and_fwd_flag")
sampledf10 = sampledf10.withColumnRenamed(' dropoff_longitude',"dropoff_longitude")
sampledf10 = sampledf10.withColumnRenamed(' dropoff_latitude',"dropoff_latitude")
sampledf10 = sampledf10.withColumnRenamed(' payment_type',"payment_type")
sampledf10 = sampledf10.withColumnRenamed(' fare_amount',"fare_amount")
sampledf10 = sampledf10.withColumnRenamed(' surcharge',"surcharge")
sampledf10 = sampledf10.withColumnRenamed(' mta_tax',"mta_tax")
sampledf10 = sampledf10.withColumnRenamed(' tip_amount',"tip_amount")
sampledf10 = sampledf10.withColumnRenamed(' tolls_amount',"tolls_amount")
sampledf10 = sampledf10.withColumnRenamed(' total_amount',"total_amount")
sampledf10 = sampledf10.withColumnRenamed(' pickup_datetime',"pickup_datetime")
sampledf10 = sampledf10.withColumnRenamed(' dropoff_datetime',"dropoff_datetime")
sampledf10.write.parquet("s3a://nycproject23/sampledrawdata/df10.parquet",mode="overwrite")

sampledf11 = spark.read.option("header",True).csv("s3a://nycproject23/rawdata/yellow_tripdata_2014-11.csv")
import pyspark.sql.functions as F
sampledf11 = sampledf11.sample(False, 0.50, seed=0)
sampledf11 = sampledf11.withColumnRenamed(' passenger_count',"passenger_count")
sampledf11 = sampledf11.withColumnRenamed(' trip_distance',"trip_distance")
sampledf11 = sampledf11.withColumnRenamed(' pickup_longitude',"pickup_longitude")
sampledf11 = sampledf11.withColumnRenamed(' pickup_latitude',"pickup_latitude")
sampledf11 = sampledf11.withColumnRenamed(' rate_code',"rate_code")
sampledf11 = sampledf11.withColumnRenamed(' store_and_fwd_flag',"store_and_fwd_flag")
sampledf11 = sampledf11.withColumnRenamed(' dropoff_longitude',"dropoff_longitude")
sampledf11 = sampledf11.withColumnRenamed(' dropoff_latitude',"dropoff_latitude")
sampledf11 = sampledf11.withColumnRenamed(' payment_type',"payment_type")
sampledf11 = sampledf11.withColumnRenamed(' fare_amount',"fare_amount")
sampledf11 = sampledf11.withColumnRenamed(' surcharge',"surcharge")
sampledf11 = sampledf11.withColumnRenamed(' mta_tax',"mta_tax")
sampledf11 = sampledf11.withColumnRenamed(' tip_amount',"tip_amount")
sampledf11 = sampledf11.withColumnRenamed(' tolls_amount',"tolls_amount")
sampledf11 = sampledf11.withColumnRenamed(' total_amount',"total_amount")
sampledf11 = sampledf11.withColumnRenamed(' pickup_datetime',"pickup_datetime")
sampledf11 = sampledf11.withColumnRenamed(' dropoff_datetime',"dropoff_datetime")
sampledf11.write.parquet("s3a://nycproject23/sampledrawdata/df11.parquet",mode="overwrite")

sampledf12 = spark.read.option("header",True).csv("s3a://nycproject23/rawdata/yellow_tripdata_2014-12.csv")
import pyspark.sql.functions as F
sampledf12 = sampledf12.sample(False, 0.50, seed=0)
sampledf12 = sampledf12.withColumnRenamed(' passenger_count',"passenger_count")
sampledf12 = sampledf12.withColumnRenamed(' trip_distance',"trip_distance")
sampledf12 = sampledf12.withColumnRenamed(' pickup_longitude',"pickup_longitude")
sampledf12 = sampledf12.withColumnRenamed(' pickup_latitude',"pickup_latitude")
sampledf12 = sampledf12.withColumnRenamed(' rate_code',"rate_code")
sampledf12 = sampledf12.withColumnRenamed(' store_and_fwd_flag',"store_and_fwd_flag")
sampledf12 = sampledf12.withColumnRenamed(' dropoff_longitude',"dropoff_longitude")
sampledf12 = sampledf12.withColumnRenamed(' dropoff_latitude',"dropoff_latitude")
sampledf12 = sampledf12.withColumnRenamed(' payment_type',"payment_type")
sampledf12 = sampledf12.withColumnRenamed(' fare_amount',"fare_amount")
sampledf12 = sampledf12.withColumnRenamed(' surcharge',"surcharge")
sampledf12 = sampledf12.withColumnRenamed(' mta_tax',"mta_tax")
sampledf12 = sampledf12.withColumnRenamed(' tip_amount',"tip_amount")
sampledf12 = sampledf12.withColumnRenamed(' tolls_amount',"tolls_amount")
sampledf12 = sampledf12.withColumnRenamed(' total_amount',"total_amount")
sampledf12 = sampledf12.withColumnRenamed(' pickup_datetime',"pickup_datetime")
sampledf12 = sampledf12.withColumnRenamed(' dropoff_datetime',"dropoff_datetime")
sampledf12.write.parquet("s3a://nycproject23/sampledrawdata/df12.parquet",mode="overwrite")

#Cleaning data
#Read parquet files from S3 into spark dataframe
df1 = spark.read.option("header",True).parquet("s3a://nycproject23/sampledrawdata/*.parquet")

#Filtering Process
df2 = df1.filter("passenger_count != '208'")

df3 = df2.filter((df2.rate_code!='156') & (df2.rate_code!='208') & (df2.rate_code!='210') & (df2.rate_code!='28') & (df2.rate_code!='65') & (df2.rate_code!='77') & (df2.rate_code!='7') & (df2.rate_code!='8') & (df2.rate_code!='9') & (df2.rate_code!='0') & (df2.rate_code!='16'))

#Drop column(beacuse more than 50% values in this column contains NULL values) 
final_df = df3.drop('store_and_fwd_flag')

#To remove dataframe null records(0.50% data null values).
final_df = final_df.na.drop()

#Transformations on pickup and dropoff datetime column(timestamp).
from pyspark.sql.functions import *
#Extract new columns from existing columns.
df1 = final_df.withColumn('pickup_hour',hour(final_df.pickup_datetime))
df1 =df1.withColumn('dropoff_hour',hour(df1.dropoff_datetime))
df1 = df1.withColumn('pickup_day',date_format(col("pickup_datetime"),"EEEE"))
df1 = df1.withColumn('dropoff_day',date_format(col("dropoff_datetime"),"EEEE"))
df1 = df1.withColumn('pickup_year',year(df1.pickup_datetime))
df1 = df1.withColumn('dropoff_year',year(df1.dropoff_datetime))
df1 = df1.withColumn('pickup_month',month(df1.pickup_datetime))
df1 = df1.withColumn('dropoff_month',month(df1.dropoff_datetime))

#Register a Dataframe as table.
df1.createOrReplaceTempView("taxitable")

#Creating hive table
sqlContext.sql("Create table nyctaxi select * from taxitable")

```

## We have used AWS CloudFormation to automate above process
[CloudFormation templet](https://github.com/bhavesh677/007/blob/Bhavesh-007/Final%20Project%2023/Code/CFT.json)
```json
{
    "AWSTemplateFormatVersion": "2010-09-09",
    "Parameters": {
        "InstanceType": {
            "Default": "m4.xlarge",
            "Type": "String"
        },
        "ReleaseLabel": {
            "Default": "emr-5.32.0",
            "Type": "String"
        },
        "SubnetId": {
            "Default": "subnet-5276aa63",
            "Type": "String"
        },
        "TerminationProtected": {
            "Type": "String",
            "Default": "false"
        },
        "ElasticMapReducePrincipal": {
            "Default": "elasticmapreduce.amazonaws.com",
            "Type": "String"
        },
        "Ec2Principal": {
            "Default": "ec2.amazonaws.com",
            "Type": "String"
        },
        "EMRLogDir": {
            "Description": "Log Dir for the EMR cluster",
            "Default": "s3://aws-logs-749482940850-us-east-1/elasticmapreduce/",
            "Type": "String"
        },
        "KeyName": {
            "Description": "Name of an existing EC2 KeyPair to enable SSH to the instances",
            "Default": "project",
            "Type": "String"
        }
    },
    "Resources": {
        "cluster": {
            "Type": "AWS::EMR::Cluster",
            "Properties": {
                "Applications": [
                    {
                        "Name": "Hadoop"
                    },
                    {
                        "Name": "Hive"
                    },
                    {
                        "Name": "Spark"
                    },
                    {
                        "Name": "Zeppelin"
                    },
                    {
                        "Name": "ZooKeeper"
                    }
                ],
                "Instances": {
                    "MasterInstanceGroup": {
                        "InstanceCount": 1,
                        "InstanceType": {
                            "Ref": "InstanceType"
                        },
                        "Market": "ON_DEMAND",
                        "Name": "Master"
                    },
                    "CoreInstanceGroup": {
                        "InstanceCount": 2,
                        "InstanceType": {
                            "Ref": "InstanceType"
                        },
                        "Market": "ON_DEMAND",
                        "Name": "Core"
                    },
                    "TerminationProtected": {
                        "Ref": "TerminationProtected"
                    },
                    "Ec2SubnetId": {
                        "Ref": "SubnetId"
                    },
                    "Ec2KeyName": {
                        "Ref": "KeyName"
                    }
                },
                "LogUri": {
                    "Ref": "EMRLogDir"
                },
                "Name": "NYCTAXI",
                "JobFlowRole": {
                    "Ref": "emrEc2InstanceProfile"
                },
                "ServiceRole": {
                    "Ref": "emrRole"
                },
                "ReleaseLabel": {
                    "Ref": "ReleaseLabel"
                },
                "VisibleToAllUsers": true,
                "Tags": [
                    {
                        "Key": "key1",
                        "Value": "value1"
                    }
                ]
            }
        },
        "emrRole": {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "AssumeRolePolicyDocument": {
                    "Version": "2008-10-17",
                    "Statement": [
                        {
                            "Sid": "",
                            "Effect": "Allow",
                            "Principal": {
                                "Service": {
                                    "Ref": "ElasticMapReducePrincipal"
                                }
                            },
                            "Action": "sts:AssumeRole"
                        }
                    ]
                },
                "Path": "/",
                "ManagedPolicyArns": [
                    "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
                ]
            }
        },
        "emrEc2Role": {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "AssumeRolePolicyDocument": {
                    "Version": "2008-10-17",
                    "Statement": [
                        {
                            "Sid": "",
                            "Effect": "Allow",
                            "Principal": {
                                "Service": {
                                    "Ref": "Ec2Principal"
                                }
                            },
                            "Action": "sts:AssumeRole"
                        }
                    ]
                },
                "Path": "/",
                "ManagedPolicyArns": [
                    "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
                ]
            }
        },
        "emrEc2InstanceProfile": {
            "Type": "AWS::IAM::InstanceProfile",
            "Properties": {
                "Path": "/",
                "Roles": [
                    {
                        "Ref": "emrEc2Role"
                    }
                ]
            }
        },
        "TestStep": {
            "Properties": {
                "ActionOnFailure": "CANCEL_AND_WAIT",
                "HadoopJarStep": {
                    "Args": [
                        "s3://nycproject23/dataingestion.sh"
                    ],
                    "Jar": "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar"
                },
                "JobFlowId": {
                    "Ref": "cluster"
                },
                "Name": "TestStep"
            },
            "Type": "AWS::EMR::Step"
        },
        "SparkStep": {
            "Properties": {
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Args": [
                        "spark-submit",
                        "--deploy-mode",
                        "cluster",
                        "--conf",
                        "spark.sql.catalogImplementation=hive",
                        "s3://nycproject23/finalpyscript.py"
                    ],
                    "Jar": "command-runner.jar"
                },
                "JobFlowId": {
                    "Ref": "cluster"
                },
                "Name": "SparkStep"
            },
            "Type": "AWS::EMR::Step"
        }
    }
}
```

## Challanges Faced:
**Slow query performance**
- Because of big volume(30GB) of our data,the performance of our query become very poor, we tackled this problem with the help of diffrent bigdata file formats(orc,parquet,etc.)
- After converting to parquet format our data volume reduced to 8GB, and performance improved.

**Emr steps**
- Initially we were strugling alot with EMR steps, but after reading AWS documentation and doing some trial and erros this problem got solved.

**CloudFormation**
- Our EMR cluster use to through some errors while we were doing cloudeFormation, but after backtracking the error message, we were able to launch EMR cluster successfully using CFT.

**Loading data in Tableau**
- This was biggest chalange in front of us to connect and load the data with tableau,even after converting our data to bigdata file formats(orc,parquet), volume of our data was still very large for the tableau to execute the query and do visualizations, we solved this problem by saving extract of our data in local machine, and then everything went very smooth.

## DATA VISUALIZATION DASHBOARD
<img src="/Final project/Images/Dashboard images/1.jpg" width=800>

<img src="/Final project/Images/Dashboard images/2.jpg" width=800>

<img src="/Final project/Images/Dashboard images/3.jpg" width=800>

<img src="/Final project/Images/Dashboard images/4.jpg" width=800>

<img src="/Final project/Images/Dashboard images/5.jpg" width=800>

<img src="/Final project/Images/Dashboard images/6.jpg" width=800>

<img src="/Final project/Images/Dashboard images/7.jpg" width=800>
