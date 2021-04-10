# Udacity Project: Stock Screenr
Tbd

**Data analysis enablement goals**  
- load data from S3 
- query song play data, meaning songs that users are listing to.  

**Initial situation**
- no easy way to query song play data due to source data structure.  
- directory of JSON logs on user activity on the app.  
- directory with JSON metadata on the songs in their app.  


## Purpose of database and design
The purpose of the resulting parquet files on AWS S3 is to ease queries on song play data. The stored data is extracted from JSON user log files and JSON song metadata files.  
The S3 files are comparable to tables of a database. If it would a database, its design would be considered a classic star structure. The fact parquet file at the center is "songplays". It references 4 dimension parquet files: users, songs, artists, time.

### File table "songplays" (parquet)
As mentioned above this is fact file which enables easy queries on songs that users are listing to.  
Its elements (column_name data_type) are:  
- songplay_id INT: automatically generated for each load from a user log file.
- start_time TIMESTAMP: reference to fact table "time"
- user_id INT: reference to fact table "users"
- level VARCHAR
- song_id VARCHAR: reference to fact table "songs"
- artist_id VARCHAR: reference to fact table "artist_id"
- session_id INT
- location VARCHAR
- user_agent VARCHAR

### Dimension file "users" (parquet)
Dimension file that stores user data.
- user_id INT,\
- first_name VARCHAR,\
- last_name VARCHAR,\
- gender VARCHAR,\
- level VARCHAR NOT NULL
                                                        
### Dimension file "songs" (parquet)
Dimension file that stores song data.
- song_id VARCHAR,\
- title VARCHAR,\
- artist_id VARCHAR NOT NULL,\
- year INT,\
- duration NUMERIC
                                                        
### Dimension file "artists" (parquet)
Dimension file that stores artist data.
- artist_id VARCHAR,\
- name VARCHAR,\
- location VARCHAR,\
- latitude NUMERIC(9,5),\
- longitude NUMERIC(9,5))

### Dimension file "time" (parquet)
Dimension file which describes time and data information based on the provided timestamp.
- start_time TIMESTAMP,\
- hour INT,\
- day INT,\
- week INT,\
- month INT,\
- year INT,\
- weekday INT

## ETL process
- The ETL process is done via two functions in the script etl.py, "process_song_data"  and "process_log_data". 
- a function create_spark_session creates a "local" Spark session and returns it.
- A main function specifies input, output locations and calls these processing functions.

### "process_song_data": ETL processing to files songs and artists
- extracts JSON song data from AWS S3, transforms it in Spark and loads the PARQUET files songs and artists to the AWS S3 target locations.
- files in the AWS S3 target location are overwritten.

### "process_log_data": transform and load data into tables
- extracts JSON song data from AWS S3 and the songs PARQUET file, transforms it in Spark and loads the PARQUET files users, time and songplays to the AWS S3 target locations.
- files in the AWS S3 target location are overwritten.

## How to interact with project repository
This sections describes:  
- how to deploy the current project version and 
- how to further develop the project

### File and folder overview
- documentation "README.MD": this file
- configuration "dl.cfg": contains the required credentials for a connection to the AWS (not included)!.
- module "etl.py": see section above on ETL process.

### Deployment of current project version as Notebook on AWS EMR

- Connection to Spark EMR cluster is automatically created. SparkSession is available as 'spark'.
- Consequently the function create_spark_session must not be used.
- AWS authentification is done via the Service IAM roles for the EMR Notebook. Because of the that configuration file and the read from the it must not be done.


## OPTIONAL: Question for the reviewer
 
If you have any question about the starter code or your own implementation, please add it in the cell below. 

For example, if you want to know why a piece of code is written the way it is, or its function, or alternative ways of implementing the same functionality, or if you want to get feedback on a specific part of your code or get feedback on things you tried but did not work.

Please keep your questions succinct and clear to help the reviewer answer them satisfactorily. 

> **_Your question_**