import os

# Import function 
from function_connection import *
from function_source_to_staging import *
from function_etl_to_dwh import *

from dotenv import load_dotenv

# Load file .env di direktori yang sama
load_dotenv()

if load_dotenv() == False:
    print('File .env tidak ditemukan!')
else:
    print(".env ditemukan")

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


    # Connection for DB STAGING
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

    # CONNECTION UNTUK DB DATAWAREHOUSE
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




    # IF CONNECTION SUCCESS, INTEGRATE TO DB STAGING
    if connection_stg[1] == True:
        
        # CONNECTION STATUS
        print(connection_stg[2])


        # TRUNCATE TABLE STAGING
        sql =[
            f"TRUNCATE TABLE {DB_SCHEMA_STG}.superstore",
            f"TRUNCATE TABLE {DB_SCHEMA_STG}.people",
            f"TRUNCATE TABLE {DB_SCHEMA_STG}.returns"
        ]

        for i in sql:
            cursor_stg.execute(i)
            conn_stg.commit()
        print('\nTRUNCATE STAGING\n')


        print("Import file to Staging")
        import_excel_to_stg(cursor = cursor_stg, DB_SCHEMA= DB_SCHEMA_STG, PATH_TO_EXCEL = PATH_TO_EXCEL)
        import_json_to_stg(cursor = cursor_stg, DB_SCHEMA= DB_SCHEMA_STG, PATH_TO_JSON = PATH_TO_JSON)
        import_xml_to_stg(cursor = cursor_stg, DB_SCHEMA= DB_SCHEMA_STG, PATH_TO_XML = PATH_TO_XML)
        conn_stg.commit()

        truncate_dwh = [
        f"truncate table {DB_SCHEMA_DWH}.customer cascade",
        f"truncate table {DB_SCHEMA_DWH}.err_log cascade",
        f"truncate table {DB_SCHEMA_DWH}.orders cascade",
        f"truncate table {DB_SCHEMA_DWH}.product cascade",
        f"truncate table {DB_SCHEMA_DWH}.region_mgr cascade",
        f"truncate table {DB_SCHEMA_DWH}.location cascade"]
        print("\nTRUNCATE DWH")
        
        for i in truncate_dwh:
            cursor_dwh.execute(i)

        print("\nImport Data to DataWarehouse")
        # STAGING TO DWH
        region_mgr(cursor_stg = cursor_stg,
                cursor_dwh = cursor_dwh,
                    DB_SCHEMA_STG = DB_SCHEMA_STG,
                    DB_SCHEMA_DWH = DB_SCHEMA_DWH
                    )
        location(cursor_stg = cursor_stg,
                cursor_dwh = cursor_dwh,
                    DB_SCHEMA_STG = DB_SCHEMA_STG,
                    DB_SCHEMA_DWH = DB_SCHEMA_DWH
                    )

        product(cursor_stg = cursor_stg,
                cursor_dwh = cursor_dwh,
                    DB_SCHEMA_STG = DB_SCHEMA_STG,
                    DB_SCHEMA_DWH = DB_SCHEMA_DWH
                    )
        
        customer(cursor_stg = cursor_stg,
                cursor_dwh = cursor_dwh,
                    DB_SCHEMA_STG = DB_SCHEMA_STG,
                    DB_SCHEMA_DWH = DB_SCHEMA_DWH
                    )
        
        orders(cursor_stg = cursor_stg,
                cursor_dwh = cursor_dwh,
                    DB_SCHEMA_STG = DB_SCHEMA_STG,
                    DB_SCHEMA_DWH = DB_SCHEMA_DWH
                    )

        conn_dwh.commit()

    else:
        print(connection_stg[2])