"""
Combined ETL DAG: Extract from SQL Server → Load to local PostgreSQL 18
One complete pipeline: Truncate → Extract → Load
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

# SQL Server config
SQL_CONFIG = {
    "DRIVER": os.getenv("SQL_DRIVER", "{ODBC Driver 18 for SQL Server}"),
    "SERVER": os.getenv("SQL_SERVER", "localhost"),
    "DATABASE": os.getenv("SQL_DATABASE", "AdventureWorksOLTP"),
    "TRUSTED_CONN": os.getenv("SQL_TRUSTED_CONN", "yes"),
    "TRUST_CERT": os.getenv("SQL_TRUST_CERT", "yes"),
    "UID": os.getenv("SQL_UID", ""),
    "PWD": os.getenv("SQL_PWD", "")
}

# Docker postgres (intermediate - where extraction loads)
DOCKER_POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "airflow"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow")
}

# Local PostgreSQL 18 (final destination)
LOCAL_POSTGRES_CONFIG = {
    "host": os.getenv("LOCAL_POSTGRES_HOST", "host.docker.internal"),
    "port": int(os.getenv("LOCAL_POSTGRES_PORT", "1223")),
    "dbname": os.getenv("LOCAL_POSTGRES_DB", "AdventureWorksOLTP"),
    "user": os.getenv("LOCAL_POSTGRES_USER", "postgres"),
    "password": os.getenv("LOCAL_POSTGRES_PASSWORD", "postgres")
}

TEST_MODE = os.getenv("TEST_MODE", "no").lower() in ("1", "yes", "true")

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def get_sql_server_conn_str() -> str:
    server = SQL_CONFIG["SERVER"]
    if ',' not in server and '\\' in server:
        parts = server.split('\\')
        server = f"{parts[0]},1433"
    elif ',' not in server:
        server = f"{server},1433"

    conn = (
        f"DRIVER={SQL_CONFIG['DRIVER']};"
        f"SERVER={server};"
        f"DATABASE={SQL_CONFIG['DATABASE']};"
        f"TrustServerCertificate={SQL_CONFIG['TRUST_CERT']};"
    )
    if SQL_CONFIG["TRUSTED_CONN"].lower() == 'no':
        conn += f"UID={SQL_CONFIG['UID']};PWD={SQL_CONFIG['PWD']};"
    else:
        conn += f"Trusted_Connection=yes;"
    return conn

def extract_table_sql_server(query: str) -> pd.DataFrame:
    """Extract from SQL Server using pyodbc."""
    if TEST_MODE:
        logger.info("TEST_MODE active: returning sample DataFrame")
        return pd.DataFrame([{"ProductID": 1, "Name": "Test Product", "Price": 9.99}])

    import pyodbc
    conn_str = get_sql_server_conn_str()
    logger.info(f"Connecting to SQL Server: {SQL_CONFIG['SERVER']}")
    with pyodbc.connect(conn_str, timeout=30) as conn:
        df = pd.read_sql(query, conn)
    return df

def load_to_docker_postgres(df: pd.DataFrame, table: str):
    """Load dataframe to Docker postgres (intermediate staging)."""
    if df.empty:
        logger.info(f"No rows to load into {table}")
        return 0

    conn = psycopg2.connect(**DOCKER_POSTGRES_CONFIG)
    cur = conn.cursor()
    
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
        sql = f"INSERT INTO {table} ({col_names}) VALUES ({values_clause});"
        cur.execute(sql)
    
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Loaded {len(df)} rows into {table} (Docker postgres)")
    return len(df)

def load_to_local_postgres(source_table: str, dest_table: str):
    """Load from Docker postgres to local PostgreSQL 18."""
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
        
        # Insert row by row
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
        
        logger.info(f"Loaded {len(df)} rows into {dest_table} (local PostgreSQL 18)")
        return len(df)
        
    except Exception as e:
        logger.error(f"Error loading {source_table}: {e}")
        raise

# ---------------------------------------------------------------------------
# Stage 1: Truncate
# ---------------------------------------------------------------------------

def truncate_docker_postgres(**context):
    """Truncate Docker postgres stg tables before extraction."""
    import psycopg2
    
    conn = psycopg2.connect(**DOCKER_POSTGRES_CONFIG)
    cur = conn.cursor()
    
    tables = ['stg.dimproduct', 'stg.dimcustomer', 'stg.dimterritory', 'stg.factsales']
    for table in tables:
        try:
            cur.execute(f"TRUNCATE TABLE {table};")
            logger.info(f"Truncated {table}")
        except Exception as e:
            logger.warning(f"Could not truncate {table}: {e}")
    
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Truncate complete")

# ---------------------------------------------------------------------------
# Stage 2: Extraction from SQL Server
# ---------------------------------------------------------------------------

def extract_product(**context):
    q = """
    SELECT 
        p.ProductID,
        p.Name as ProductName,
        p.ProductNumber,
        psc.Name as SubcategoryName,
        psc2.Name as CategoryName,
        p.ListPrice
    FROM Production.Product p
    LEFT JOIN Production.ProductSubcategory psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
    LEFT JOIN Production.ProductCategory psc2 ON psc.ProductCategoryID = psc2.ProductCategoryID
    """
    df = extract_table_sql_server(q)
    df.columns = ['productid', 'productname', 'productnumber', 'subcategoryname', 'categoryname', 'listprice']
    n = load_to_docker_postgres(df, 'stg.dimproduct')
    logger.info(f"extract_product: loaded {n} rows to Docker postgres")

def extract_customer(**context):
    q = """
    SELECT 
        c.CustomerID,
        c.AccountNumber,
        COALESCE(CONCAT(p.FirstName, ' ', p.LastName), s.Name) as CustomerName,
        CASE WHEN p.BusinessEntityID IS NOT NULL THEN 'Individual' ELSE 'Store' END as CustomerType
    FROM Sales.Customer c
    LEFT JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
    LEFT JOIN Sales.Store s ON c.StoreID = s.BusinessEntityID
    """
    df = extract_table_sql_server(q)
    df.columns = ['customerid', 'accountnumber', 'customername', 'customertype']
    n = load_to_docker_postgres(df, 'stg.dimcustomer')
    logger.info(f"extract_customer: loaded {n} rows to Docker postgres")

def extract_sales(**context):
    q = """
    SELECT 
        CAST(soh.OrderDate AS DATE) as OrderDate,
        sod.ProductID,
        soh.CustomerID,
        soh.TerritoryID,
        sod.OrderQty,
        sod.UnitPrice,
        sod.UnitPriceDiscount,
        sod.LineTotal
    FROM Sales.SalesOrderDetail sod
    JOIN Sales.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
    """
    df = extract_table_sql_server(q)
    df.columns = ['orderdate', 'productid', 'customerid', 'territoryid', 'orderqty', 'unitprice', 'unitpricediscount', 'linetotal']
    n = load_to_docker_postgres(df, 'stg.factsales')
    logger.info(f"extract_sales: loaded {n} rows to Docker postgres")

def extract_territory(**context):
    q = """
    SELECT 
        TerritoryID,
        Name as TerritoryName,
        CountryRegionCode
    FROM Sales.SalesTerritory
    """
    df = extract_table_sql_server(q)
    df.columns = ['territoryid', 'territoryname', 'countryregioncode']
    n = load_to_docker_postgres(df, 'stg.dimterritory')
    logger.info(f"extract_territory: loaded {n} rows to Docker postgres")

# ---------------------------------------------------------------------------
# Stage 3: Loading to local PostgreSQL 18
# ---------------------------------------------------------------------------

def load_dimproduct(**context):
    load_to_local_postgres("stg.dimproduct", "stg.dimproduct")

def load_dimcustomer(**context):
    load_to_local_postgres("stg.dimcustomer", "stg.dimcustomer")

def load_dimterritory(**context):
    load_to_local_postgres("stg.dimterritory", "stg.dimterritory")

def load_factsales(**context):
    load_to_local_postgres("stg.factsales", "stg.factsales")

# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id='combined_full_etl',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=2)},
    max_active_runs=1,
) as dag:

    # Stage 1: Truncate
    truncate = PythonOperator(task_id='truncate_stg', python_callable=truncate_docker_postgres)
    
    # Stage 2: Extract
    extract_prod = PythonOperator(task_id='extract_product', python_callable=extract_product)
    extract_cust = PythonOperator(task_id='extract_customer', python_callable=extract_customer)
    extract_sales_task = PythonOperator(task_id='extract_sales', python_callable=extract_sales)
    extract_terr = PythonOperator(task_id='extract_territory', python_callable=extract_territory)
    
    # Stage 3: Load
    load_prod = PythonOperator(task_id='load_dimproduct', python_callable=load_dimproduct)
    load_cust = PythonOperator(task_id='load_dimcustomer', python_callable=load_dimcustomer)
    load_terr = PythonOperator(task_id='load_dimterritory', python_callable=load_dimterritory)
    load_sales = PythonOperator(task_id='load_factsales', python_callable=load_factsales)
    
    # Dependencies:
    # Stage 1 → Stage 2: Truncate before all extracts
    truncate >> [extract_prod, extract_cust, extract_sales_task, extract_terr]
    
    # Stage 2 → Stage 3: All extracts must complete before loads
    extract_prod >> load_prod
    extract_cust >> load_cust
    extract_terr >> load_terr
    [extract_sales_task, load_prod, load_cust, load_terr] >> load_sales
