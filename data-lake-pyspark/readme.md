# Project 4: Data Lake & Spark

<p align="center"><img src="/data-lake-pyspark/images/spark-logo.png" style="height: 100%; width: 100%; max-width: 400px" /></p>


## Introduction
Sparkify has now reached a level of maturity that the need for a data lake in a modern distributed filesystem is obvious. Enter Apache Spark - PySpark. This distributed computing library allows for processing massive amounts of data with a simple API and output its results to the filesystem of choice. It can be run locally or on a cluster of machines in the cloud with as many nodes as desired. 

Modern day clouds make it easy to set-up a cluster with hundreds or tens of nodes easily. Using a serive like AWS EMR allows for this cluster to be self managed, removing a lot of overhead from the data engineers, which should be more worried about optimizing their code to process the data fast and accurately.

Sparkify has not produced enough data to be consumed by reporting tools and analyzed by experts. Unfortunately, this data is saved in JSON files, without a optimized for analysis schema. This data is updated daily. The need for an ETL to transform this data to a Star Schema and saved in a new format (parquet) is present.

This project intends to set up a ETL process, using PySpark, to move the data from a S3 location in JSON format to a second location, with optimized partioned mode and in the Parquet format to be readiyl consumed by reporting tools.

## AWS Infrastructure
In this project it is recommended to run the ETL in a EMR cluster (m3.xlarge) - This allows for the best performance and pricing. Also, ensure that the IAM role utilized has all the necessary permission to access S3 to write the data

# Run The Scripts
Use python to run `etl.py` - this will run the full ETL processing.

**Observation**: Ensure that the environment running the routine has all necessary libraries installed. If using EMR, recommended, submit the job to the spark driver.

# Datasets Analysis
### Song Dataset
The songs data is originated from  [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong). 
All data is in JSON format and partioned accordingly to the first letter of the track ID. It can be found in S3 with in the following manner:

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
Each file has the following content:
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```
### Log Dataset
This dataset is obtained from a fake data generator and simulates users usage of the app. These log files are partioned by year and month (Only 2018 is present in the raw dataset)

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

# Output Schema
The output schema of this ETL will be a optimized star schema with one-to-one relationships with the driver table

<p align="center"><img src="/data-lake-pyspark/images/star-schema.png" width="50%"/>

#### Fact Table
1. songplays - records related to songs played
    * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables
1. <b>users</b> - Users Information
    * user_id, first_name, last_name, gender, level
2. <b>songs</b> - Songs Information
    * song_id, title, artist_id, year, duration
3. <b>artists</b> - Artists Information
    * artist_id, name, location, lattitude, longitude
4. <b>time</b> - Timestamps and associated time metrics
    * start_time, hour, day, week, month, year, weekday

# Observations
EMR clusters and S3 sometimes do not play nicely together. What should be a quick easy job, sometimes takes a long time to write to S3. There are many reasons why discussed [here](https://stackoverflow.com/questions/42822483/extremely-slow-s3-write-times-from-emr-spark), [here](https://stackoverflow.com/questions/40830152/how-to-avoid-reading-old-files-from-s3-when-appending-new-data/41284043#41284043), [here](https://stackoverflow.com/questions/36927918/using-spark-to-write-a-parquet-file-to-s3-over-s3a-is-very-slow), [here](https://stackoverflow.com/questions/31817143/spark-write-parquet-to-s3-the-last-task-takes-forever) and [here](https://stackoverflow.com/questions/40408574/slow-parquet-write-to-hdfs-using-spark?rq=1)

Recommendation: Do not use the output path of S3 as `s3a://your-path/here` but like `s3://your-path/here`. While running this on EMR, `s3a` format would take hours while `s3` took a handful of seconds. If the problem still persists, create an EMR with HDFS attached and write to it. Then copy the results to S3 using [this](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/UsingEMR_s3distcp.html)

[Lunching Jupyter Notebook on EMR](https://medium.com/@christo.lagali/run-jupyter-notebooks-with-pyspark-on-an-emr-cluster-9630ef54c4e1)

[Installing PyPi libraries on EMR with PySpark Kernel](https://aws.amazon.com/blogs/big-data/install-python-libraries-on-a-running-cluster-with-emr-notebooks/)

Run `sparkify-etl.ipynb` on a EMR cluster