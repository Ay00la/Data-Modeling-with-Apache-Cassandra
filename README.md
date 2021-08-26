# Data Modeling with Apache Cassandra
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer questions.

## Project Overview
This purpose of this project is to model data with Apache Cassandra and build an ETL pipeline using Python's pandas and cassandra package. The ETL pipeline transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

## Dataset
The dataset used for this project are the sparkify CSV file located in **event_data/** folder.

## Environment
* Python 3.6 or above
* Apache Cassandra 3.11.11 or above
* Jupyter notebook
* cassandra - cassandra database Python package.

## Details of the project
In this project, the questions to be answered (i.e., the queries to be ran on the database tables) were considered before creating the tables on the **sparkify_k** database.

Details of the Data Modeling were explained in **Project.ipynb** notebook
