from google.cloud import bigquery
from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv




def upload_to_dwh_cloud(
                        prm_PROJECT_ID = None,
                        prm_DATASET_ID = None,
                        prm_CLIENT = None,
                        prm_CURSOR_DWH = None,
                        prm_DB_SCHEMA_DWH = None
                        ):


    table_names = ["region_mgr", "location", "product", "customer", "orders"]

    for names in table_names:

        table_id = f"{prm_PROJECT_ID}.{prm_DATASET_ID}.{names}"

        # Query select table
        query = f"SELECT * FROM {prm_DB_SCHEMA_DWH}.{names}"

        prm_CURSOR_DWH.execute(query)

        rows = prm_CURSOR_DWH.fetchall()

        # Ambil nama kolom
        column_names = [desc[0] for desc in prm_CURSOR_DWH.description]

        # Konversi ke DataFrame
        df = pd.DataFrame(rows, columns=column_names)


        # Konfigurasi job untuk upload ke BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Timpa data jika ada
            autodetect=True,  # Deteksi schema secara otomatis
        )

        # Upload dataframe ke BigQuery
        job = prm_CLIENT.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Tunggu hingga selesai

    print(f"Upload sukses ke {table_id}. Total baris: {df.shape[0]}")


