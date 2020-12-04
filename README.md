Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

Project Description
In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

To do this essay, it will be used the star schema which has the main fact and dimensions that they are joined to the fact. This schema is called like that because it gets the shape of a star. This is the easiest way to handle whole data which will be piped from dimensions to fact and those datas can be checked easily.

Project Datasets
You'll be working with two datasets that reside in S3. Here are the S3 links for each:

LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

The achivement is create stament in sql_queries.py to create each table and run create_table.py to create the data base and tables and finally run the test.ioynb to confirm the creation of your tablewith the correct columns.

The py file, which is called sql_queries, is file the sequences that must be used to create the tables through a cluster from Amazon Web Side. The tables can be created through the py file that is called create_table, and first of all, there is to connect to the cluster from Amazon Web Side how to was said before. The second step, it will be dropped the tables through the function that is called drop_tables and so it can avoid that there is a table which is called with the same name that the future tables thas they will be created. The third step, it will be created the tables which they will be used in the star schema that is called create_tables. At this step,the tables, which are called "songplays", "users", "songs", "artists" and "time", will be created.

When the tables have been created, the py file, which is called etl, is run and its objective is to get every data from JSON file which is named above and where this JSON file gets every data from customers and songs and to do this, first of all, there is to connect to the cluster from Amazon Web Side and the first step for this, it is to run py file that will be the first run through the function load_staging_tables where costumers data and song data are copied into the Redshift and then trough the function insert_tables, the data are loaded in the table that is assigned.
