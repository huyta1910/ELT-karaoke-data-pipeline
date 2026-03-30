import os
import pyodbc
import pandas as pd
import pandas_gbq
from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import logging
from typing import List, Dict, Any
from datetime import datetime, timezone
import concurrent.futures
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Traverse up one folder to find the .env file globally when executed by Airflow inside /opt/airflow/karaoke/ingestion
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path if env_path.exists() else None)

# Dynamically override the GCP Credentials so it resolves correctly in BOTH Windows and Docker Linux
cred_path = Path(__file__).resolve().parent.parent / "credentials.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(cred_path)

SQL_SERVER = os.getenv('SQL_SERVER_HOST')
SQL_DATABASE = os.getenv('SQL_DATABASE')
SQL_USERNAME = os.getenv('SQL_USERNAME')
SQL_PASSWORD = os.getenv('SQL_PASSWORD')

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BQ_RAW_DATASET = os.getenv('BQ_RAW_DATASET', 'analytics_raw')

CHUNKSIZE = 100000
LOAD_TIMESTAMP_COLUMN = 'loaded_at_ts'

# --- CONFIGURATION ---
TABLES_TO_INGEST: List[Dict[str, Any]] = [
    {"source_name": "dbo.HoaDon_ChiTietHangHoa", "dest_name": "raw_hoadon_chitiethanghoa", "pk": "id", "source_date_col": "created_date"},
    {"source_name": "dbo.CuaHang", "dest_name": "raw_cuahang", "pk": "id", "source_date_col": "created_date"},
    {"source_name": "dbo.Ca", "dest_name": "raw_ca", "pk": "id", "source_date_col": "created_date"},
    {"source_name": "dbo.HoaDon", "dest_name": "raw_transactions", "pk": "id", "source_date_col": "created_date"},
    {"source_name": "dbo.KhachHang", "dest_name": "raw_customer", "pk": "id", "source_date_col": "created_date"},
    {"source_name": "dbo.HangHoa", "dest_name": "raw_hanghoa", "pk": "id", "source_date_col": "created_date"},
    {"source_name": "dbo.HoaDon_Combo_HangHoa", "dest_name": "raw_hoadon_combo_hanghoa", "pk": "id", "source_date_col": "created_date"},
    {"source_name": "dbo.HoaDon_GiamGia", "dest_name": "raw_hoadon_giamgia", "pk": "id", "source_date_col": "created_date"}
]

def get_sql_connection_string() -> str:
    preferred_drivers = ["ODBC Driver 17 for SQL Server", "ODBC Driver 18 for SQL Server"]
    driver_to_use = next((driver for driver in preferred_drivers if driver in pyodbc.drivers()), None)
    
    if not driver_to_use:
        available = pyodbc.drivers()
        if available: driver_to_use = available[0]
        else: raise RuntimeError("No ODBC drivers found.")

    return (
        f"DRIVER={{{driver_to_use}}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};Encrypt=no;TrustServerCertificate=yes;"
    )

def get_last_pipeline_run_time(client: bigquery.Client, final_table_id: str):
    """
    Checks BigQuery to find the MAX(loaded_at_ts).
    Uses the pipeline's own timestamp to avoid bad source data (e.g. year 2116) breaking the watermark.
    """
    try:
        client.get_table(final_table_id)
        query = f"SELECT MAX(loaded_at_ts) as last_run FROM `{final_table_id}`"
        job = client.query(query)
        result = list(job.result())
        
        if result and result[0].last_run is not None:
            return result[0].last_run
            
    except NotFound:
        return None 
    except Exception as e:
        logging.warning(f"Could not get last run time: {e}. Defaulting to full load.")
        return None
    return None

def process_table(client: bigquery.Client, table_config: Dict[str, Any]):
    dest_name = table_config['dest_name']
    source_name = table_config['source_name']
    source_date_col = table_config.get('source_date_col', 'created_date') 
    
    # We write DIRECTLY to the final table now
    final_table_id = f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{dest_name}"
    dataset_table_str = f"{BQ_RAW_DATASET}.{dest_name}" # For pandas-gbq

    # 1. GET LAST RUN TIME
    last_run_time = get_last_pipeline_run_time(client, final_table_id)
    
    # 2. BUILD SQL QUERY
    if last_run_time is not None:
        val_str = last_run_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        logging.info(f"Incremental Load: Fetching {source_name} where {source_date_col} > '{val_str}'")
        sql_query = f"SELECT * FROM {source_name} WHERE {source_date_col} > '{val_str}'"
    else:
        logging.info(f"Full Load: No previous run found for {dest_name}.")
        sql_query = f"SELECT * FROM {source_name}"

    # 3. LOAD DATA (APPEND ONLY)
    conn_str = get_sql_connection_string()
    total_rows = 0
    current_load_timestamp = datetime.now(timezone.utc)

    try:
        with pyodbc.connect(conn_str, autocommit=True) as conn:
            chunk_iterator = pd.read_sql(sql_query, conn, chunksize=CHUNKSIZE)
            
            def process_and_upload(df, chunk_index):
                if df.empty: return 0
                df.columns = [col.lower().replace(' ', '_').replace('[', '').replace(']', '') for col in df.columns]
                df[LOAD_TIMESTAMP_COLUMN] = current_load_timestamp
                
                logging.info(f"Uploading chunk {chunk_index+1} ({len(df)} rows) directly to {final_table_id}...")
                pandas_gbq.to_gbq(
                    df,
                    destination_table=dataset_table_str,
                    project_id=GCP_PROJECT_ID,
                    if_exists='append'
                )
                logging.info(f"Chunk {chunk_index+1} uploaded successfully.")
                return len(df)
                
            # Process the very first chunk synchronously. This guarantees the BigQuery table 
            # is created safely and prevents the 'Table already exists' race condition.
            try:
                first_chunk = next(chunk_iterator)
                total_rows += process_and_upload(first_chunk, 0)
            except StopIteration:
                logging.info(f"No new records to append for {dest_name}.")
                
            if total_rows > 0:
                # We use ThreadPoolExecutor to upload any remaining chunks in parallel 
                # This overlaps network upload with database reading
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                    futures = []
                    for i, chunk_df in enumerate(chunk_iterator, start=1):
                        if chunk_df.empty: continue
                        
                        logging.info(f"Read chunk {i+1} from SQL. Submitting to background upload task...")
                        # Submit background upload task
                        futures.append(executor.submit(process_and_upload, chunk_df, i))
                    
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            total_rows += future.result()
                        except Exception as e:
                            logging.error(f"Error uploading chunk: {e}")
                            raise

        logging.info(f"Successfully Appended {total_rows} rows to {dest_name}.")

    except Exception as e:
        logging.error(f"Failed to process {dest_name}: {e}")
        raise

def main():
    logging.info("--- Starting Free-Tier Compatible Pipeline ---")
    bigquery_client = bigquery.Client(project=GCP_PROJECT_ID)
    try: bigquery_client.get_dataset(BQ_RAW_DATASET)
    except NotFound: bigquery_client.create_dataset(BQ_RAW_DATASET)
        
    for table_config in TABLES_TO_INGEST:
        process_table(bigquery_client, table_config)

if __name__ == "__main__":
    main()