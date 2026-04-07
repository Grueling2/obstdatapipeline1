Data engineering platform simulating a multi‑location bookstore. Includes event streaming (Redpanda/Kafka), distributed processing (Spark), workflow orchestration (Airflow), Delta Lake (Bronze/Silver), MinIO S3 storage, MariaDB SQL integration, and Gold analytics via Jupyter.

This project simulates an end-to-end data engineering pipeline for a simulated three-location bookstore company in Wisconsin.
The custom Docker container 'bookstore_producer' simulates business events with weights based on store location and categories of business events (browsing, returned items, sales, add-to-cart).
The full pipeline accounts for
Raw business event creation, event streaming, distributed processing, workflow orchestration, lakehouse modeling and analytics.

Infrastructure is a fully containerized environment, established via docker-compose.yml, as well as secrets stored in .env and spark/conf/spark-defaults.conf  

<img width="217" height="643" alt="image" src="https://github.com/user-attachments/assets/9e6c4665-3bbd-44af-b59b-e36c409ee8a0" />



Technologies Used:  
-Redpanda/Kafka: event streaming  
-Apache Spark: distributed ETL  
-Apache Airflow: orchestration  
-Delta Lake: ACID lakehouse storage  
-MinIO, S3 compatible object store  
-MariaDB: reference data  
-Jupyter Notebooks: analytics  
-Docker Compose: infrastructure automation  
-Python: pipeline logic  

Workflow automation: Apache Airflow DAGs which orchestrate the following pipelines  

dag_catalog_loader:  
1. Bookstore_producer container runs catalog_loader.py script  
2. NYT API grabs real catalog of best selling books in various genres  
3. MariaDB tables get updated with new catalog entries  
<img width="1727" height="883" alt="image" src="https://github.com/user-attachments/assets/728c1f3e-e7f3-455c-83d1-e4a09aaf2e4c" />  
  
  
dag_run_bookstore_simulation_day_cycle:  
1. Bookstore_producer container runs bookstore_producer.py script, beginning simulated business day  
2. Redpanda/Kafka raw business events get generated continuously for a simulated 8-hour business day  
3. Bookstore_producer.py script is stopped  
4. Popularity values based on simulated day sales get updated in MariaDB  
5. Increment day_index to prepare for next business day  
<img width="1727" height="883" alt="image" src="https://github.com/user-attachments/assets/d2409924-4116-407e-b547-71cef819ad66" />  
  
  
dag_spark_job_build_bronze:  
1. Spark Streaming reads Redpanda/Kafka raw business events  
2. Delta Lake Bronze table data is written continuously with structured streaming on MinIO S3 bucket  
<img width="1728" height="886" alt="image" src="https://github.com/user-attachments/assets/f32a5180-6c2a-4af0-a69d-2718b41ee8c0" />  
  
  
dag_spark_job_build_silver-sales and dag_spark_job_build_silver_returns:  
1. Spark Streaming reads Delta Lake Bronze table data for that partitioned business day  
2. Delta lake Silver table with just sales data and just returns data is written continuously with structured streaming on MinIO S3 bucket  
<img width="1728" height="886" alt="image" src="https://github.com/user-attachments/assets/e5f2056c-19b5-4112-9708-282d0b17bd33" />  
  
  

Redpanda Raw Events Example:  
<img width="540" height="277" alt="image" src="https://github.com/user-attachments/assets/2d460222-fc5a-403b-ab46-eed8aa3854db" />  

Example Silver Sales query results:  
<img width="659" height="322" alt="image" src="https://github.com/user-attachments/assets/39ad1f78-0d36-4c82-b9f9-10c84d8b5941" />  
<img width="663" height="326" alt="image" src="https://github.com/user-attachments/assets/e3f7d900-2b24-462b-8e97-c8da8388df75" />  
<img width="661" height="324" alt="image" src="https://github.com/user-attachments/assets/558de41c-83be-4e56-8e7b-fbc46dbe21b9" />  


Example Schema for books table in MariaDB:  
<img width="1632" height="191" alt="image" src="https://github.com/user-attachments/assets/4d6a5add-b1a2-4fad-b7c0-cc6e1b2f0490" />  

Final data result, in addition to Bronze/Silver tables:  
Gold business metrics visualized in Jupyter Notebooks  
1. Total sales today  
2. Sales over time, split up by store  
3. Top 3 books sold today  
4. Biggest single increase, and biggest single decrease in popularity of a book over the preceding 7 days.  
5. Top 5 best-selling authors over the preceding 7 days.  

Example snippet from Gold Jupyter Notebook  
<img width="1264" height="734" alt="image" src="https://github.com/user-attachments/assets/264fba1b-5acb-4b06-8669-80936e467790" />

 
