"""
Simplified, resilient ETL DAG for AdventureWorks-like data.
- Modular: uses helper functions for connections and ETL
- Test-mode: can use local SQLite fallback if SQL Server is not available
- Loads data into Postgres (Airflow metadata DB usable for demo)
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

# Put .env two levels up detection
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)
else:
    load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Simple config
SQL_CONFIG = {
    "DRIVER": os.getenv("SQL_DRIVER", "{ODBC Driver 18 for SQL Server}"),
    "SERVER": os.getenv("SQL_SERVER", "localhost"),
    "DATABASE": os.getenv("SQL_DATABASE", "AdventureWorksOLTP"),
    "TRUSTED_CONN": os.getenv("SQL_TRUSTED_CONN", "yes"),
    "TRUST_CERT": os.getenv("SQL_TRUST_CERT", "yes"),
    "UID": os.getenv("SQL_UID", ""),
    "PWD": os.getenv("SQL_PWD", "")
}

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "airflow"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow")
}

TEST_MODE = os.getenv("TEST_MODE", "no").lower() in ("1", "yes", "true")

def _debug_env():
    logger.info(f"TEST_MODE={TEST_MODE}")
    logger.info(f"SQL_SERVER={SQL_CONFIG['SERVER']}")
    logger.info(f"SQL_DATABASE={SQL_CONFIG['DATABASE']}")

# Optionally log environment at module import to assist with debugging
_debug_env()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_sql_server_conn_str() -> str:
    server = SQL_CONFIG["SERVER"]
    if ',' not in server and '\\' in server:
        # named instance -> assume TCP 1433 to keep it simple
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
    """Extract using pyodbc from SQL Server. In test mode, returns sample data."""
    if TEST_MODE:
        logger.info("TEST_MODE active: returning sample DataFrame")
        return pd.DataFrame([{"ProductID": 1, "Name": "Test Product", "Price": 9.99}])

    import pyodbc
    conn_str = get_sql_server_conn_str()
    logger.info(f"Connecting to SQL Server with: {conn_str}")
    with pyodbc.connect(conn_str, timeout=30) as conn:
        df = pd.read_sql(query, conn)
    return df


def load_to_postgres(df: pd.DataFrame, table: str):
    """Load dataframe to Postgres using psycopg2 (insert into existing schema.table)."""
    import psycopg2

    if df.empty:
        logger.info(f"No rows to load into {table}")
        return 0

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    
    # Insert row by row to handle NULLs and type conversions properly
    col_names = ", ".join(df.columns)
    for _, row in df.iterrows():
        # Build VALUES clause with NULL for NaN/empty values
        values = []
        for val in row:
            if pd.isna(val) or val == '':
                values.append("NULL")
            else:
                # Convert to string and escape single quotes
                val_str = str(val).replace("'", "''")
                values.append(f"'{val_str}'")
        values_clause = ", ".join(values)
        sql = f"INSERT INTO {table} ({col_names}) VALUES ({values_clause});"
        cur.execute(sql)
    
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Loaded {len(df)} rows into {table}")
    return len(df)

def truncate_stg_tables(**context):
    """Truncate stg tables before extraction."""
    import psycopg2
    
    conn = psycopg2.connect(**POSTGRES_CONFIG)
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
# ETL tasks
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
    # Rename columns to match staging table
    df.columns = ['productid', 'productname', 'productnumber', 'subcategoryname', 'categoryname', 'listprice']
    n = load_to_postgres(df, 'stg.dimproduct')
    logger.info(f"extract_product: loaded {n} rows")


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
    # Rename columns to match staging table
    df.columns = ['customerid', 'accountnumber', 'customername', 'customertype']
    n = load_to_postgres(df, 'stg.dimcustomer')
    logger.info(f"extract_customer: loaded {n} rows")


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
    # Rename columns to match staging table
    df.columns = ['orderdate', 'productid', 'customerid', 'territoryid', 'orderqty', 'unitprice', 'unitpricediscount', 'linetotal']
    n = load_to_postgres(df, 'stg.factsales')
    logger.info(f"extract_sales: loaded {n} rows")


def extract_territory(**context):
    q = """
    SELECT 
        TerritoryID,
        Name as TerritoryName,
        CountryRegionCode
    FROM Sales.SalesTerritory
    """
    df = extract_table_sql_server(q)
    # Rename columns to match staging table
    df.columns = ['territoryid', 'territoryname', 'countryregioncode']
    n = load_to_postgres(df, 'stg.dimterritory')
    logger.info(f"extract_territory: loaded {n} rows")


with DAG(
    dag_id='simple_dw_etl',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=2)},
    max_active_runs=1,
) as dag:

    t0 = PythonOperator(task_id='truncate_stg_tables', python_callable=truncate_stg_tables)
    t1 = PythonOperator(task_id='extract_product', python_callable=extract_product)
    t2 = PythonOperator(task_id='extract_customer', python_callable=extract_customer)
    t3 = PythonOperator(task_id='extract_sales', python_callable=extract_sales)
    t4 = PythonOperator(task_id='extract_territory', python_callable=extract_territory)

    t0 >> t1 >> t2 >> t3
    t0 >> t4
