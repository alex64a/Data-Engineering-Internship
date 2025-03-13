
# Data Engineering Internship

<p align="left">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20NiFi-0066CC?style=for-the-badge&logo=apache-nifi&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white"/>
  <img src="https://img.shields.io/badge/MinIO-990000?style=for-the-badge&logo=minio&logoColor=white"/>
  <img src="https://img.shields.io/badge/PySpark-FDEE21?style=for-the-badge&logo=apachespark&logoColor=black"/>
  <img src="https://img.shields.io/badge/Metabase-509EE3?style=for-the-badge&logo=metabase&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white"/>
  <img src="https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white"/>
</p>


Internship task @ https://functorns.com/
## Project I

### Summary

The project is designed to include key aspects in the field of
Data Engineering:
* collection and storage of raw data,
* creation and management of data flows,
* processing of collected data in batch and streaming mode,
* storage of the obtained processing results in destination
  databases (SQL/noSQL),
* automation and orchestration of the collection and processing
  process,
* tabular and graphical presentation of results to the end user.
### Phase I - Access to data sources
Data sources that will be used for the project are:
* YouTube - https://developers.google.com/youtube/v3/getting-started
* Twitter (X) - https://developer.x.com/en 
* Kaggle - https://www.kaggle.com/


### Phase II - Data Collection
After providing access to the data from the given source, the data
needs to be retrieved and placed/forwarded to the appropriate
storage. 

Implemented a docker compose file with following images: 
* Nifi (for data ingestion)
* Spark (and Spark-Worker)
* Airflow (for Spark-job automation) 
* Kafka (with Zookeeper)
* Minio (S3 Storage)
* PostgreSQL (Data Warehouse)
* Metabase (Data Visualization)

### Nifi Flow

<p align="left">
  <img src="https://github.com/user-attachments/assets/972eb668-2615-48c7-8441-9ce4c64b55d9" width="500" alt="Nifi Flow">
</p>

* **ExecuteStreamCommand processor** - executes python script that aquires data (Youtube, Twitter etc.).
* **ConvertRecord processor** - converts data from json to avro/parquet.
* **SplitJson processor** - splits large json response into multiple single responses.
* **PutS3 processor** - stores data to a S3 Bucket in Minio for batch processing.
* **PublishKafka processor** - sends data to a kafka topic for stream processing.

### Phase III - batch data processing
After collecting and storing the raw data in the Data Lake (MinIO), it is necessary to clean and transform the raw data, as
well as place it in the destination storage/database.

Implemented Spark applications to: clean, transform the Data Lake data and write it to a PostgreSQL database.
Automated all the spark jobs with Apache Airflow : 
<p align="left">
  <img src="https://github.com/user-attachments/assets/aec9579a-7be2-4545-8697-9aaec9cd4405" width="500", height="300">
  <img src="https://github.com/user-attachments/assets/8f79b19f-a014-42e8-b48c-9e317708f0fe" width="500", height="300">
</p>

### Phase IV - streaming data processing
Collected data that has been aggregated into data streams needs to
be processed in streaming mode. The results of streaming
processing must be placed in new data streams or a suitable real-
time storage.

Implemented a Spark streaming application to clean, transform data from a Kafka topic and merge the resulting data streams into an appropriate database.

<p align="left">
  <img src="https://github.com/user-attachments/assets/c68640e9-d843-4c7a-8970-18cba92d2094" width="500">
</p>

### Phase V - presentation of results
It is necessary to present the processing results to the user in
the most natural way possible, through interactive tables and
diagrams. Metabase was used for data visualization.
<p align="left">
  <img src="https://github.com/user-attachments/assets/b6643dea-51b1-4c2f-a0e9-aad5564c0868" width="500", height="240">
  <img src="https://github.com/user-attachments/assets/445b985c-582d-424f-8544-b6fb7a2124ec" width="500", height="240">
</p>
<p align="left">
  <img src="https://github.com/user-attachments/assets/9ca0194e-95b6-40cf-a64c-6325b67d1745" width="500", height="240">
  <img src="https://github.com/user-attachments/assets/f00feca3-08de-4c24-a8c6-391ce72fca6c" width="500", height="240">
</p>

