"""
Load data from Docker postgres stg schema to local PostgreSQL 18 AdventureWorksOLTP database.
This DAG depends on the extraction DAG completing first.
"""
from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import psycopg2

# Load .env
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)
else:
    load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Docker postgres (source - where extraction loads)
DOCKER_POSTGRES_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow"
}

# Local PostgreSQL 18 (destination)
LOCAL_POSTGRES_CONFIG = {
    "host": os.getenv("LOCAL_POSTGRES_HOST", "host.docker.internal"),
    "port": int(os.getenv("LOCAL_POSTGRES_PORT", "1223")),
    "dbname": os.getenv("LOCAL_POSTGRES_DB", "AdventureWorksOLTP"),
    "user": os.getenv("LOCAL_POSTGRES_USER", "postgres"),
    "password": os.getenv("LOCAL_POSTGRES_PASSWORD", "postgres")
}

def load_table(source_table: str, dest_table: str, **context):
    """Read from Docker postgres stg and write to local PostgreSQL 18."""
    try:
        # Read from Docker postgres
        logger.info(f"Reading from Docker postgres: {source_table}")
        source_conn = psycopg2.connect(**DOCKER_POSTGRES_CONFIG)
        df = pd.read_sql(f"SELECT * FROM {source_table}", source_conn)
        source_conn.close()
        
        if df.empty:
            logger.warning(f"No data found in {source_table}")
            return 0
        
        logger.info(f"Read {len(df)} rows from {source_table}")
        
        # Write to local PostgreSQL 18
        logger.info(f"Writing to local PostgreSQL 18: {dest_table}")
        dest_conn = psycopg2.connect(**LOCAL_POSTGRES_CONFIG)
        dest_cur = dest_conn.cursor()
        
        # Truncate destination table
        dest_cur.execute(f"TRUNCATE TABLE {dest_table}")
        
        # Insert row by row to handle NULLs properly
        col_names = ", ".join(df.columns)
        for _, row in df.iterrows():
            values = []
            for val in row:
                if pd.isna(val) or val == '':
                    values.append("NULL")
                else:
                    val_str = str(val).replace("'", "''")
                    values.append(f"'{val_str}'")
            values_clause = ", ".join(values)
            sql = f"INSERT INTO {dest_table} ({col_names}) VALUES ({values_clause});"
            dest_cur.execute(sql)
        
        dest_conn.commit()
        dest_cur.close()
        dest_conn.close()
        
        logger.info(f"Loaded {len(df)} rows into {dest_table}")
        return len(df)
        
    except Exception as e:
        logger.error(f"Error loading {source_table}: {e}")
        raise

def load_dimproduct(**context):
    load_table("stg.dimproduct", "stg.dimproduct", **context)

def load_dimcustomer(**context):
    load_table("stg.dimcustomer", "stg.dimcustomer", **context)

def load_dimterritory(**context):
    load_table("stg.dimterritory", "stg.dimterritory", **context)

def load_factsales(**context):
    load_table("stg.factsales", "stg.factsales", **context)

with DAG(
    dag_id='load_stg_to_local_postgres',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=2)},
    max_active_runs=1,
) as dag:

    t1 = PythonOperator(task_id='load_dimproduct', python_callable=load_dimproduct)
    t2 = PythonOperator(task_id='load_dimcustomer', python_callable=load_dimcustomer)
    t3 = PythonOperator(task_id='load_dimterritory', python_callable=load_dimterritory)
    t4 = PythonOperator(task_id='load_factsales', python_callable=load_factsales)

    [t1, t2, t3] >> t4
