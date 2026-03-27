from google.cloud import bigquery
import os
from dotenv import load_dotenv

load_dotenv()
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BQ_RAW_DATASET = os.getenv('BQ_RAW_DATASET', 'analytics_raw')

client = bigquery.Client(project=GCP_PROJECT_ID)

tables = [
    "raw_hoadon_chitiethanghoa",
    "raw_cuahang",
    "raw_ca",
    "raw_transactions",
    "raw_customer"
]

print("--- BigQuery Incremental Load Status ---")
for table in tables:
    table_id = f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{table}"
    try:
        query = f"SELECT COUNT(*) as cnt, MAX(loaded_at_ts) as last_load FROM `{table_id}`"
        results = list(client.query(query).result())
        for row in results:
            print(f"Table: {table}")
            print(f"  Rows: {row.cnt}")
            print(f"  Last Load: {row.last_load}")
    except Exception as e:
        print(f"Table {table}: Not found or error: {e}")
