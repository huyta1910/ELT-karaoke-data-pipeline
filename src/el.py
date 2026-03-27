import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import logging
from typing import List, Dict, Any
from datetime import datetime, timezone
import concurrent.futures

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

load_dotenv()

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
    {"source_name": "dbo.KhachHang", "dest_name": "raw_customer", "pk": "id", "source_date_col": "created_date"}
    # {"source_name": "dbo.HoaDon_GiamGia", "dest_name": "raw_sql_hoadon_giamgia", "pk": "id", "source_date_col": "created_date"}
]

def get_sql_connection_string() -> str:
    preferred_drivers = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]
    driver_to_use = next((driver for driver in preferred_drivers if driver in pyodbc.drivers()), None)
    
    if not driver_to_use:
        available = pyodbc.drivers()
        if available: driver_to_use = available[0]
        else: raise RuntimeError("No ODBC drivers found.")

    return (
        f"DRIVER={{{driver_to_use}}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=yes;"
    )

def get_last_pipeline_run_time(client: bigquery.Client, final_table_id: str, date_col: str):
    """
    Checks BigQuery to find the MAX(source_date_col).
    This accurately tracks the newest record from the source system.
    """
    try:
        client.get_table(final_table_id)
        # Format the column name the same way pandas dataframe columns are formatted
        bq_date_col = date_col.lower().replace(' ', '_').replace('[', '').replace(']', '')
        query = f"SELECT MAX({bq_date_col}) as last_run FROM `{final_table_id}`"
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
    last_run_time = get_last_pipeline_run_time(client, final_table_id, source_date_col)
    
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
            
            def upload_chunk(df, chunk_index):
                logging.info(f"Uploading chunk {chunk_index+1} ({len(df)} rows) directly to {final_table_id}...")
                # --- CHANGE: Write directly to final table, Append mode ---
                # This uses 'Load Jobs' which are allowed in Free Tier
                df.to_gbq(
                    destination_table=dataset_table_str,
                    project_id=GCP_PROJECT_ID,
                    if_exists='append'
                )
                logging.info(f"Chunk {chunk_index+1} uploaded successfully.")
                return len(df)
                
            # We use ThreadPoolExecutor to upload chunks to BigQuery in parallel 
            # This overlaps network upload with database reading
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                futures = []
                for i, chunk_df in enumerate(chunk_iterator):
                    if chunk_df.empty: continue
                    
                    chunk_df.columns = [col.lower().replace(' ', '_').replace('[', '').replace(']', '') for col in chunk_df.columns]
                    chunk_df[LOAD_TIMESTAMP_COLUMN] = current_load_timestamp
                    
                    logging.info(f"Read chunk {i+1} from SQL. Submitting to background upload task...")
                    # Submit background upload task
                    futures.append(executor.submit(upload_chunk, chunk_df, i))
                
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