#!/bin/sh

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-01.csv
hdfs dfs -put yellow_tripdata_2014-01.csv s3a://nycproject23/rawdatas3/
rm yellow_tripdata_2014-01.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-02.csv
hdfs dfs -put yellow_tripdata_2014-02.csv s3a://nycproject23/rawdatas3/
rm yellow_tripdata_2014-02.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-03.csv
hdfs dfs -put yellow_tripdata_2014-03.csv s3a://nycproject23/rawdatas3/
rm yellow_tripdata_2014-03.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-04.csv
hdfs dfs -put yellow_tripdata_2014-04.csv s3a://nycproject23/rawdatas3/
rm yellow_tripdata_2014-04.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-05.csv
hdfs dfs -put yellow_tripdata_2014-05.csv s3a://nycproject23/rawdatas3/
rm yellow_tripdata_2014-05.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-06.csv
hdfs dfs -put yellow_tripdata_2014-06.csv s3a://nycproject23/rawdatas3/
rm yellow_tripdata_2014-06.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-07.csv
hdfs dfs -put yellow_tripdata_2014-07.csv s3a://nycproject23/rawdatas3/
rm yellow_tripdata_2014-07.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-08.csv
hdfs dfs -put yellow_tripdata_2014-08.csv s3a://nycproject23/rawdatas3/
rm yellow_tripdata_2014-08.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-09.csv
hdfs dfs -put yellow_tripdata_2014-09.csv s3a://nycproject23/rawdatas3/
rm yellow_tripdata_2014-09.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-10.csv
hdfs dfs -put yellow_tripdata_2014-10.csv s3a://nycproject23/rawdatas3/
rm yellow_tripdata_2014-10.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-11.csv
hdfs dfs -put yellow_tripdata_2014-11.csv s3a://nycproject23/rawdatas3/
rm yellow_tripdata_2014-11.csv

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2014-12.csv
hdfs dfs -put yellow_tripdata_2014-12.csv s3a://nycproject23/rawdatas3/
rm yellow_tripdata_2014-12.csv


 
