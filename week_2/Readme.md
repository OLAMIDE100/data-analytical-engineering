# 🏹Week 2 ( Data Ingestion) Summary:
<hr>

<hr>
🎯Conceptual understanding of Docker networks, how they connect together to execute tasks.



🎯Understanding the cost-benefit behind converting our dataset from csv format to parquet form when storing on cloud and how to successfully carryout such conversion using PYARROW, as storage affect both time and money.



🎯Conceptual understanding of DAG and its operators, service provider’s operators, Email operators, Sensors.

 

🎯Setting up the airflow environment on docker with google Cloud SDK successfully installed for ingesting various CSV files into Data lake (google cloud bucket) and big query (data warehouse).

 

🎯Modified the data ingestion scripts created in week 1 to accommodate parameters to enable us to incorporate them as a function in the PYTHONOPERATOR tasks of our DAG.

 

🎯Scheduling Tasks using Linux Cron and orchestrating our pipelines for both local Postgres and cloud storage ingesting using Apache Airflow.



🎯 Moving dataset from AWS s3 to GC bucket using google transfer service at both the console and terraform environment.



🎯Successfully ingested NY Taxi Trips (2019 – 2020), Zone lookup, for-hire vehicle trip datasets as parquet files into our cloud bucket.



🎯Optimized a multi-low airflow config and cost-saving environment (single node setup ) more suited for our machine and local development.

<hr>
