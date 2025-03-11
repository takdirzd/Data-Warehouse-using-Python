import os
import datetime

# Import function
from f_connect_postgres import *
from f_source_to_staging import *
from f_etl_to_dwh import *

from f_upload_bigquery import *

from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

# Import Library bigquery
from google.cloud import bigquery

# Load file .env
load_dotenv()

# Load from .env
DB_HOSTNAME = os.getenv('DB_HOSTNAME')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAMES')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_SCHEMA_STG = os.getenv('DB_SCHEMA_STG')
DB_SCHEMA_DWH = os.getenv('DB_SCHEMA_DWH')
PATH_TO_EXCEL = os.getenv('PATH_TO_EXCEL')
PATH_TO_JSON = os.getenv('PATH_TO_JSON')
PATH_TO_XML = os.getenv('PATH_TO_XML')

# For BigQuery
PROJECT_ID_BIGQUERY = os.getenv('PROJECT_ID_BQ')
DATASET_ID_BIGQUERY = os.getenv('DATASET_ID_BQ')
KEYFILE_PATH_BIGQUERY = os.getenv('KEYFILE_PATH_BQ')


if load_dotenv() == False:
    print('File .env tidak ditemukan!')
else:
    print(".env ditemukan")

    ######################################################
    #                   CONNECTION                       #
    ######################################################

    # CONNECTION FOR DB STAGING LOCAL
    connection_stg = func_db_connect(
                        prm_db_host = DB_HOSTNAME,
                        prm_db_port = DB_PORT, 
                        prm_db_name = DB_NAME, 
                        prm_user = DB_USERNAME,
                        prm_pass = DB_PASSWORD,
                        prm_schema = DB_SCHEMA_STG,
                    )


    conn_stg = connection_stg[0]
    cursor_stg = conn_stg.cursor()

    # CONNECTION FOR DB DATAWAREHOUSE LOCAL
    connection_stg = func_db_connect(
                        prm_db_host = DB_HOSTNAME,
                        prm_db_port = DB_PORT, 
                        prm_db_name = DB_NAME, 
                        prm_user = DB_USERNAME,
                        prm_pass = DB_PASSWORD,
                        prm_schema = DB_SCHEMA_DWH,
                    )

    conn_dwh = connection_stg[0]
    cursor_dwh = conn_dwh.cursor()

    # CONNECTION FOR CLOUD DATAWAREHOUSE
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEYFILE_PATH_BIGQUERY

    # Inisialisasi client BigQuery
    client_bigquery = bigquery.Client(project=PROJECT_ID_BIGQUERY)



    ######################################################
    #                   INTEGRATION                      #
    ######################################################

    #######################
    #       STAGING       #
    #######################
    def truncate_staging():
        """TRUNCATE STAGING"""
        
        sql = [
            f"TRUNCATE TABLE {DB_SCHEMA_STG}.superstore",
            f"TRUNCATE TABLE {DB_SCHEMA_STG}.people",
            f"TRUNCATE TABLE {DB_SCHEMA_STG}.returns"
        ]
        
        for query in sql:
            cursor_stg.execute(query)

        conn_stg.commit()
        print('\nSTAGING TRUNCETED\n')

    # Source to Staging
    def import_data_staging():
        """Import file to Staging"""

        import_excel_to_stg(cursor_stg, DB_SCHEMA_STG, PATH_TO_EXCEL)
        import_json_to_stg(cursor_stg, DB_SCHEMA_STG, PATH_TO_JSON)
        import_xml_to_stg(cursor_stg, DB_SCHEMA_STG, PATH_TO_XML)

        conn_stg.commit()

    def truncate_dwh():
        """TRUNCATE DWH"""

        sql = [
            f"TRUNCATE TABLE {DB_SCHEMA_DWH}.customer CASCADE",
            f"TRUNCATE TABLE {DB_SCHEMA_DWH}.err_log CASCADE",
            f"TRUNCATE TABLE {DB_SCHEMA_DWH}.orders CASCADE",
            f"TRUNCATE TABLE {DB_SCHEMA_DWH}.product CASCADE",
            f"TRUNCATE TABLE {DB_SCHEMA_DWH}.region_mgr CASCADE",
            f"TRUNCATE TABLE {DB_SCHEMA_DWH}.location CASCADE"
        ]

        for query in sql:
            cursor_dwh.execute(query)

        conn_dwh.commit()
        print('\DWH TRUNCETED\n')


    #######################
    #       DWH LOCAL     #
    #######################
    def load_data_to_dwh():
        """Transfer data from Staging to temporary Datawarehouse"""

        region_mgr(cursor_stg, cursor_dwh, DB_SCHEMA_STG, DB_SCHEMA_DWH)
        location(cursor_stg, cursor_dwh, DB_SCHEMA_STG, DB_SCHEMA_DWH)
        product(cursor_stg, cursor_dwh, DB_SCHEMA_STG, DB_SCHEMA_DWH)
        customer(cursor_stg, cursor_dwh, DB_SCHEMA_STG, DB_SCHEMA_DWH)
        orders(cursor_stg, cursor_dwh, DB_SCHEMA_STG, DB_SCHEMA_DWH)

        conn_dwh.commit()


    ######################################################
    #                LOAD TO CLOUD DWH                   #
    ######################################################
    def load_data_to_bigquery():

        upload_to_dwh_cloud(
                            prm_PROJECT_ID = PROJECT_ID_BIGQUERY,
                            prm_DATASET_ID = DATASET_ID_BIGQUERY,
                            prm_CLIENT = client_bigquery,
                            prm_CURSOR_DWH = cursor_dwh,
                            prm_DB_SCHEMA_DWH = DB_SCHEMA_DWH
                            )

# Definisi DAG
default_args = {
    "owner": "takdir",
    "depends_on_past": False,
    "start_date": datetime.datetime(2024, 3, 7),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1),
}

dag = DAG(
    "etl_superstore",
    default_args=default_args,
    description="DAG ETL pipeline from local source data to cloud Datawarehouse",
    schedule_interval="0 8 * * 1", # every monday 08.00 AM
    catchup=False,
)

t1 = PythonOperator(
    task_id="truncate_staging",
    python_callable=truncate_staging,
    dag=dag,
)

t2 = PythonOperator(
    task_id="import_data_staging",
    python_callable=import_data_staging,
    dag=dag,
)

t3 = PythonOperator(
    task_id="truncate_dwh",
    python_callable=truncate_dwh,
    dag=dag,
)

t4 = PythonOperator(
    task_id="load_data_to_dwh",
    python_callable=load_data_to_dwh,
    dag=dag,
)

t5 = PythonOperator(
    task_id="load_data_to_bigquery",
    python_callable=load_data_to_bigquery,
    dag=dag,
    
)

# Queue for task
t1 >> t2 >> t3 >> t4 >> t5
