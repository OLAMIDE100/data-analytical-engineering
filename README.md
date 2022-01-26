# DATA AND ANALYTICAL ENGINEERING



## Overview

### Architecture diagram
<img src="images/architecture/arch_1.jpg"/>

### Technologies
* *Google Cloud Platform (GCP)*: Cloud-based auto-scaling platform by Google
  * *Google Cloud Storage (GCS)*: Data Lake
  * *BigQuery*: Data Warehouse
* *Terraform*: Infrastructure-as-Code (IaC)
* *Docker*: Containerization
* *SQL*: Data Analysis & Exploration
* *Airflow*: Pipeline Orchestration
* *DBT*: Data Transformation
* *Spark*: Distributed Processing
* *Kafka*: Streaming

## Syllabus

### [Week 1: Introduction & Prerequisites](week_1_basics_n_setup)

* Course overview
* Introduction to GCP
* Docker and docker-compose 
* Running Postgres locally with Docker
* Setting up infrastructure on GCP with Terraform
* Preparing the environment for the course
* Homework

[Details](week_1_basics_n_setup)



### [Week 2: Data ingestion](week_2_data_ingestion)


* Introduction to Data Lake (GCS) 
  
* Introduction to Orchestration (Airflow) 
  
* Workshop:
  * Setting up Docker with Airflow:
  * Data ingestion DAG: 
    * Cloud-based, i.e. with GCP (GCS + BigQuery)
    * Local, with Postgres

* Further Enhancements: Transfer Service (AWS -> GCP)



### [Week 3: Data Warehouse](week_3_data_warehouse)


* Data warehouse (BigQuery)
    * What is a data warehouse solution
    * What is big query, why is it so fast, Cost of BQ, 
    * Partitoning and clustering, Automatic re-clustering 
    * Pointing to a location in google storage (5 min)
    * Loading data to big query & PG-- using Airflow operator?
    * BQ best practices
    * Misc: BQ Geo location, BQ ML 
    * Alternatives (Snowflake/Redshift)



### [Week 4: Analytics engineering](week_4_analytics_engineering/taxi_rides_ny/)



* Basics 
    * What is DBT?
    * ETL vs ELT 
    * Data modeling
    * DBT fit of the tool in the tech stack
* Usage (Combination of coding + theory)
    * Anatomy of a dbt model: written code vs compiled Sources
    * Materialisations: table, view, incremental, ephemeral  
    * Seeds 
    * Sources and ref  
    * Jinja and Macros 
    * Tests  
    * Documentation 
    * Packages 
    * Deployment: local development vs production 
    * DBT cloud: scheduler, sources and data catalog (Airflow)
* Google data studio -> Dashboard
* Extra knowledge:
    * DBT cli (local)




### [Week 5: Batch processing](week_5_batch_processing)


* Distributed processing (Spark) 
    * What is Spark, spark cluster
    * Explaining potential of Spark
    * What is broadcast variables, partitioning, shuffle
    * Pre-joining data 
    * use-case
    * What else is out there (Flink)
* Extending Orchestration env (airflow)
    * Big query on airflow
    * Spark on airflow 



### [Week 6: Streaming](week_6_stream_processing)



* Basics
    * What is Kafka
    * Internals of Kafka, broker
    * Partitoning of Kafka topic
    * Replication of Kafka topic
* Consumer-producer
* Schemas (avro)
* Streaming
    * Kafka streams
* Kafka connect
* Alternatives (PubSub/Pulsar)





### [Week 7, 8 & 9: Project](project)









