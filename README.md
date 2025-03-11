Data Pipeline using PostgreSQL and Google Bigquery  
  
This project develops a Data Warehouse using PostgreSQL as the local data source and Google BigQuery as the Cloud Data Warehouse. Apache Airflow is used to orchestrate the automated ETL pipeline, which includes:  
+ Extract: Fetching data from multiple sources (Excel, JSON, XML).  
+ Transform: Cleaning, validating, and processing data into the required format.  
+ Load: Loading data into the local DWH and subsequently into BigQuery.

Data-Warehouse-using-Python/  
│── dags/                          # Airflow DAGs for ETL pipeline since this project using Airflow for orchestration  
│   │── data/                        # Folder for source data  
│   │   │── superstore.xlsx          # Data contain superstore in EXCEL file  
│   │   │── people.json             # Data contain people in JSON file  
│   │   │── returns.xml             # Data contain returns in XML file  
│   │── .env                        # Environment variables for the script  
│   │── f_connect_postgres.py        # Function for PostgreSQL connection script  
│   │── f_source_to_staging.py       # Function for Extract data from source to staging  
│   │── f_etl_to_dwh.py              # Function for ETL process from staging to DWH  
│   │── f_upload_bigquery.py         # Function for Upload processed data to BigQuery  
│   │── main_dag.py                  # Main DAG orchestration script  

Data Flow Overview
![image](https://github.com/user-attachments/assets/d86d805d-928a-4545-9e22-f417b825105a)

This diagram represents the ETL (Extract, Transform, Load) data pipeline using Apache Airflow for orchestrating data movement across different layers:

+ SOURCE
    - Data sources include:
        Excel (Superstore)
        JSON (Person)
        XML (Returns)
    - Data is imported to staging using Python scripts.

+ STAGING
    - Data is temporarily stored in PostgreSQL before transformation.
    - Python scripts are used for data ingestion into the staging layer.

+ DATA WAREHOUSE
    - Transformed and cleaned data is stored in a structured PostgreSQL Data Warehouse.
    - Contain error log for Orders table that have constraint issues
    - This step ensures data consistency and integrity before further processing.

+ CLOUD DATA WAREHOUSE
    - The final transformed data is uploaded to Google BigQuery for analytics and reporting.
    - Python scripts manage data transfer from the on-premise Data Warehouse to the cloud.

Apache Airflow orchestrates the entire pipeline, automating the ETL process and scheduling from data integration to cloud storage.

DATA WAREHOUSE PROJECT.pdf for details.

Since .env can't upload, here is what it contains =>  
DB_HOSTNAME = (host name or localhost)  
DB_PORT = (port)  
DB_NAMES = (db_name)  
DB_USERNAME = (username)  
DB_PASSWORD = (password)  
DB_SCHEMA_STG = staging_superstore  
DB_SCHEMA_DWH = dwh_superstore  
PATH_TO_EXCEL = /path/to/file/Superstore.xlsx  
PATH_TO_JSON = /path/to/file/People.json  
PATH_TO_XML = /path/to/file/Returns.xml  
etc..
