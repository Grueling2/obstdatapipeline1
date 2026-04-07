Data engineering pipeline (Raw Events -> Bronze -> Silver -> Gold) simulating a multi‑location bookstore company in Wisconsin. Includes event streaming (Redpanda/Kafka), distributed processing (Spark), workflow orchestration (Airflow), Delta Lake (Bronze/Silver), MinIO S3-compatible local storage and AWS S3 cloud storage, MariaDB SQL integration, and interactive Gold business intelligence dashboards via Databricks.  
    
Project infrastructure uses containers established via docker-compose.yml, as well as secrets stored in .env and spark/conf/spark-defaults.conf  The custom Docker container 'bookstore_producer' simulates business events with various weighted preferences based on store location and categories of business events (browsing, returned items, sales, add-to-cart).  
<img width="364" height="970" alt="image" src="https://github.com/user-attachments/assets/79f4df97-5f5e-4f30-8e4d-cf4e72bb1aae" />  
  
Databricks interactive dashboard showing the following Gold-tier metrics for business stakeholders:  
1. Total sales today  
2. Sales over time, split up by store  
3. Top 3 books sold today  
4. Biggest single increase, and biggest single decrease in popularity of a book over the preceding 7 days.  
5. Top 5 best-selling authors over the preceding 7 days.  
<img width="1866" height="1040" alt="image" src="https://github.com/user-attachments/assets/51e11c01-552b-41be-a840-5886f5a05c73" />  
  
Technologies Used:  
-Databricks: analytics and business intelligence  
-Redpanda/Kafka: event streaming  
-Apache Spark: distributed ETL  
-Apache Airflow: workflow orchestration  
-Delta Lake: ACID lakehouse storage  
-MinIO: S3-compatible object store  
-AWS S3: cloud-based object store for Databricks  
-MariaDB: reference data  
-Jupyter Notebooks: analytics  
-Docker Compose: infrastructure automation  
-Python: pipeline logic  
  
Apache Airflow DAGs orchestrate the following pipelines  
  
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
6. Export MariaDB tables into CSV files and placing into MinIO for processing in a later DAG  
<img width="2070" height="760" alt="image" src="https://github.com/user-attachments/assets/2ce31e6a-9983-4995-89fc-17a83ebb5812" />  
  
  
dag_spark_job_build_bronze:  
1. Spark Streaming reads Redpanda/Kafka raw business events  
2. Delta Lake Bronze table data is written continuously with structured streaming on MinIO  
<img width="1728" height="886" alt="image" src="https://github.com/user-attachments/assets/f32a5180-6c2a-4af0-a69d-2718b41ee8c0" />  
  
  
dag_spark_job_build_silver_sales and dag_spark_job_build_silver_returns:  
1. Spark Streaming reads Delta Lake Bronze table data for that partitioned business day  
2. Delta lake Silver table with just sales data and just returns data is written continuously with structured streaming on MinIO  
<img width="1728" height="886" alt="image" src="https://github.com/user-attachments/assets/e5f2056c-19b5-4112-9708-282d0b17bd33" />


dag_sync_minio_to_aws_s3:
1. MinIO storage gets synced to AWS S3 cloud storage object store, copying Bronze and Silver Delta Lake tables with all checkpoints and metadata into AWS to be utilized in the Databricks platform catalog  
  
  
Databricks scheduled job:  
1. Collects CSV files from MinIO local storage
2. Converts into Delta Lake tables in Bronze and Silver categories on AWS S3 bucket for processing in Gold dashboard  
<img width="783" height="820" alt="image" src="https://github.com/user-attachments/assets/9fd14d0c-6a9e-420d-b421-3e44b235e811" />  
  
  
Redpanda Raw Events Example:  
<img width="540" height="277" alt="image" src="https://github.com/user-attachments/assets/2d460222-fc5a-403b-ab46-eed8aa3854db" />  
  
  
Example Silver Sales query results:  
<img width="659" height="322" alt="image" src="https://github.com/user-attachments/assets/39ad1f78-0d36-4c82-b9f9-10c84d8b5941" />  
  
  
Example Schema for books table in MariaDB:  
<img width="1632" height="191" alt="image" src="https://github.com/user-attachments/assets/4d6a5add-b1a2-4fad-b7c0-cc6e1b2f0490" />  
  
  
Example snippet from Gold Jupyter Notebook  
<img width="1264" height="734" alt="image" src="https://github.com/user-attachments/assets/264fba1b-5acb-4b06-8669-80936e467790" />
