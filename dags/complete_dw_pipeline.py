"""
Complete End-to-End Data Warehouse Pipeline
Extracts from SQL Server → Loads to Staging → Transforms with SCD → Loads to DW
Optimized with SQL JOINs for speed (handles 300K+ rows in minutes)
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

# Docker postgres (staging)
STAGING_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "airflow"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow")
}

# Local PostgreSQL 18 (DW)
DW_CONFIG = {
    "host": os.getenv("LOCAL_POSTGRES_HOST", "host.docker.internal"),
    "port": int(os.getenv("LOCAL_POSTGRES_PORT", "1223")),
    "dbname": os.getenv("LOCAL_POSTGRES_DB", "AdventureWorksOLTP"),
    "user": os.getenv("LOCAL_POSTGRES_USER", "postgres"),
    "password": os.getenv("LOCAL_POSTGRES_PASSWORD", "postgres")
}

TEST_MODE = os.getenv("TEST_MODE", "no").lower() in ("1", "yes", "true")

# ---------------------------------------------------------------------------
# Step 1: SQL Server Extraction Helper
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
        logger.info("TEST_MODE: returning sample data")
        return pd.DataFrame([{"ProductID": 1, "Name": "Test", "Price": 9.99}])

    import pyodbc
    conn_str = get_sql_server_conn_str()
    logger.info(f"Connecting to SQL Server: {SQL_CONFIG['SERVER']}")
    with pyodbc.connect(conn_str, timeout=30) as conn:
        df = pd.read_sql(query, conn)
    return df

def load_to_staging(df: pd.DataFrame, table: str):
    """Load dataframe to staging using batch INSERT."""
    if df.empty:
        logger.info(f"No rows to load into {table}")
        return 0

    conn = psycopg2.connect(**STAGING_CONFIG)
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
    logger.info(f"Loaded {len(df)} rows into {table}")
    return len(df)

# ---------------------------------------------------------------------------
# Stage 1: Truncate & Extract
# ---------------------------------------------------------------------------

def truncate_staging(**context):
    """Truncate staging tables."""
    conn = psycopg2.connect(**STAGING_CONFIG)
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

def extract_and_load_all(**context):
    """Extract all 4 tables from SQL Server and load to staging."""
    
    # 1. Extract Products
    q = """
    SELECT 
        p.ProductID, p.Name as ProductName, p.ProductNumber,
        psc.Name as SubcategoryName, psc2.Name as CategoryName, p.ListPrice
    FROM Production.Product p
    LEFT JOIN Production.ProductSubcategory psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
    LEFT JOIN Production.ProductCategory psc2 ON psc.ProductCategoryID = psc2.ProductCategoryID
    """
    df = extract_table_sql_server(q)
    df.columns = ['productid', 'productname', 'productnumber', 'subcategoryname', 'categoryname', 'listprice']
    load_to_staging(df, 'stg.dimproduct')
    
    # 2. Extract Customers
    q = """
    SELECT 
        c.CustomerID, c.AccountNumber,
        COALESCE(CONCAT(p.FirstName, ' ', p.LastName), s.Name) as CustomerName,
        CASE WHEN p.BusinessEntityID IS NOT NULL THEN 'Individual' ELSE 'Store' END as CustomerType
    FROM Sales.Customer c
    LEFT JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
    LEFT JOIN Sales.Store s ON c.StoreID = s.BusinessEntityID
    """
    df = extract_table_sql_server(q)
    df.columns = ['customerid', 'accountnumber', 'customername', 'customertype']
    load_to_staging(df, 'stg.dimcustomer')
    
    # 3. Extract Sales
    q = """
    SELECT 
        CAST(soh.OrderDate AS DATE) as OrderDate, sod.ProductID, soh.CustomerID, soh.TerritoryID,
        sod.OrderQty, sod.UnitPrice, sod.UnitPriceDiscount, sod.LineTotal
    FROM Sales.SalesOrderDetail sod
    JOIN Sales.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
    """
    df = extract_table_sql_server(q)
    df.columns = ['orderdate', 'productid', 'customerid', 'territoryid', 'orderqty', 'unitprice', 'unitpricediscount', 'linetotal']
    load_to_staging(df, 'stg.factsales')
    
    # 4. Extract Territories
    q = """
    SELECT TerritoryID, Name as TerritoryName, CountryRegionCode
    FROM Sales.SalesTerritory
    """
    df = extract_table_sql_server(q)
    df.columns = ['territoryid', 'territoryname', 'countryregioncode']
    load_to_staging(df, 'stg.dimterritory')
    
    logger.info("All extractions complete")

# ---------------------------------------------------------------------------
# Stage 2: DW Schema Initialization
# ---------------------------------------------------------------------------

def init_dw_schema(**context):
    """Initialize DW schema (only if it doesn't exist)."""
    try:
        conn = psycopg2.connect(**DW_CONFIG)
        cur = conn.cursor()
        
        # Check if schema exists
        cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'dw'")
        schema_exists = cur.fetchone() is not None
        
        if schema_exists:
            logger.info("DW schema already exists, skipping initialization")
            cur.close()
            conn.close()
            return
        
        logger.info("DW schema not found, creating...")
        schema_file = Path(__file__).parent / 'init_dw_schema.sql'
        if schema_file.exists():
            with open(schema_file, 'r') as f:
                sql = f.read()
            cur.execute(sql)
            conn.commit()
            logger.info("DW schema created successfully")
        
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error initializing DW schema: {e}")
        raise

# ---------------------------------------------------------------------------
# Stage 3: Dimension Loading (Optimized with SQL)
# ---------------------------------------------------------------------------

def load_dim_date(**context):
    """Load DimDate using SQL batch insert."""
    try:
        conn = psycopg2.connect(**DW_CONFIG)
        cur = conn.cursor()
        
        # Generate dates for 2000-2030
        date_range = pd.date_range(start='2000-01-01', end='2030-12-31', freq='D')
        
        for date in date_range:
            sql = """
            INSERT INTO dw.dimdate (date, year, month, day, quarter, dayofweek, isweekend)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING;
            """
            cur.execute(sql, (
                date.date(), date.year, date.month, date.day,
                (date.month - 1) // 3 + 1, date.weekday(), date.weekday() >= 5
            ))
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("DimDate loaded")
    except Exception as e:
        logger.error(f"Error loading DimDate: {e}")
        raise

def load_dim_product_scd2(**context):
    """Load DimProduct (SCD Type 2) - Optimized."""
    try:
        stg_conn = psycopg2.connect(**STAGING_CONFIG)
        stg_df = pd.read_sql("SELECT * FROM stg.dimproduct", stg_conn)
        
        earliest_date_df = pd.read_sql("SELECT MIN(orderdate) as min_date FROM stg.factsales", stg_conn)
        earliest_date = earliest_date_df['min_date'][0] if not earliest_date_df.empty else datetime.now().date()
        stg_conn.close()
        
        if stg_df.empty:
            logger.warning("No data in stg.dimproduct")
            return
        
        dw_conn = psycopg2.connect(**DW_CONFIG)
        dw_cur = dw_conn.cursor()
        
        for _, row in stg_df.iterrows():
            product_id, product_name, product_number = row['productid'], row['productname'], row['productnumber']
            subcategory, category, list_price = row['subcategoryname'], row['categoryname'], row['listprice']
            
            dw_cur.execute("SELECT productkey, listprice FROM dw.dimproduct WHERE productid = %s AND iscurrent = TRUE", (product_id,))
            existing = dw_cur.fetchone()
            
            if existing is None:
                dw_cur.execute(
                    "INSERT INTO dw.dimproduct (productid, productname, productnumber, subcategoryname, categoryname, listprice, validfrom, validto, iscurrent) VALUES (%s, %s, %s, %s, %s, %s, %s, NULL, TRUE)",
                    (product_id, product_name, product_number, subcategory, category, list_price, earliest_date)
                )
            elif existing[1] != list_price:
                old_productkey = existing[0]
                dw_cur.execute("UPDATE dw.dimproduct SET validto = %s, iscurrent = FALSE WHERE productkey = %s", (datetime.now().date(), old_productkey))
                dw_cur.execute(
                    "INSERT INTO dw.dimproduct (productid, productname, productnumber, subcategoryname, categoryname, listprice, validfrom, validto, iscurrent) VALUES (%s, %s, %s, %s, %s, %s, %s, NULL, TRUE)",
                    (product_id, product_name, product_number, subcategory, category, list_price, datetime.now().date())
                )
        
        dw_conn.commit()
        dw_cur.close()
        dw_conn.close()
        logger.info("DimProduct (SCD Type 2) loaded")
    except Exception as e:
        logger.error(f"Error loading DimProduct: {e}")
        raise

def load_dim_customer_scd1(**context):
    """Load DimCustomer (SCD Type 1) - Optimized with SQL UPSERT."""
    try:
        dw_conn = psycopg2.connect(**DW_CONFIG)
        dw_cur = dw_conn.cursor()
        
        # Use SQL UPSERT for speed
        sql = """
        INSERT INTO dw.dimcustomer (customerid, accountnumber, customername, customertype, loaddate, updatedate)
        SELECT customerid, accountnumber, customername, customertype, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM stg.dimcustomer
        ON CONFLICT (customerid) DO UPDATE SET
            customername = EXCLUDED.customername,
            customertype = EXCLUDED.customertype,
            updatedate = CURRENT_TIMESTAMP;
        """
        dw_cur.execute(sql)
        dw_conn.commit()
        dw_cur.close()
        dw_conn.close()
        logger.info("DimCustomer (SCD Type 1) loaded")
    except Exception as e:
        logger.error(f"Error loading DimCustomer: {e}")
        raise

def load_dim_territory_scd1(**context):
    """Load DimTerritory (SCD Type 1) - Optimized with SQL UPSERT."""
    try:
        dw_conn = psycopg2.connect(**DW_CONFIG)
        dw_cur = dw_conn.cursor()
        
        sql = """
        INSERT INTO dw.dimterritory (territoryid, territoryname, countryregioncode, loaddate, updatedate)
        SELECT territoryid, territoryname, countryregioncode, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM stg.dimterritory
        ON CONFLICT (territoryid) DO UPDATE SET
            territoryname = EXCLUDED.territoryname,
            countryregioncode = EXCLUDED.countryregioncode,
            updatedate = CURRENT_TIMESTAMP;
        """
        dw_cur.execute(sql)
        dw_conn.commit()
        dw_cur.close()
        dw_conn.close()
        logger.info("DimTerritory (SCD Type 1) loaded")
    except Exception as e:
        logger.error(f"Error loading DimTerritory: {e}")
        raise

# ---------------------------------------------------------------------------
# Stage 4: Fact Table Loading (SQL JOIN - FAST!)
# ---------------------------------------------------------------------------

def load_fact_sales(**context):
    """Load FactSales using SQL JOIN - handles 300K+ rows in seconds."""
    try:
        dw_conn = psycopg2.connect(**DW_CONFIG)
        dw_cur = dw_conn.cursor()
        
        insert_sql = """
        INSERT INTO dw.factsales 
        (datekey, productkey, customerkey, territorykey, orderqty, unitprice, unitpricediscount, linetotal, loaddate)
        SELECT
            dd.datekey, dp.productkey, dc.customerkey, dt.territorykey,
            sf.orderqty, sf.unitprice, sf.unitpricediscount, sf.linetotal, CURRENT_TIMESTAMP
        FROM stg.factsales sf
        JOIN dw.dimdate dd ON sf.orderdate = dd.date
        JOIN dw.dimproduct dp ON sf.productid = dp.productid 
            AND dp.validfrom <= sf.orderdate 
            AND (dp.validto IS NULL OR dp.validto > sf.orderdate)
            AND dp.iscurrent = TRUE
        JOIN dw.dimcustomer dc ON sf.customerid = dc.customerid
        JOIN dw.dimterritory dt ON sf.territoryid = dt.territoryid;
        """
        
        logger.info("Loading FactSales using SQL JOIN (optimized for speed)...")
        dw_cur.execute(insert_sql)
        dw_conn.commit()
        
        rows_loaded = dw_cur.rowcount
        dw_cur.close()
        dw_conn.close()
        
        logger.info(f"FactSales loaded: {rows_loaded} rows")
    except Exception as e:
        logger.error(f"Error loading FactSales: {e}")
        raise

# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id='complete_dw_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=2)},
    max_active_runs=1,
) as dag:

    # Stage 1: Extract
    truncate = PythonOperator(task_id='truncate_staging', python_callable=truncate_staging)
    extract = PythonOperator(task_id='extract_all', python_callable=extract_and_load_all)
    
    # Stage 2: Initialize DW
    init_dw = PythonOperator(task_id='init_dw_schema', python_callable=init_dw_schema)
    
    # Stage 3: Load Dimensions
    load_date = PythonOperator(task_id='load_dim_date', python_callable=load_dim_date)
    load_prod = PythonOperator(task_id='load_dim_product', python_callable=load_dim_product_scd2)
    load_cust = PythonOperator(task_id='load_dim_customer', python_callable=load_dim_customer_scd1)
    load_terr = PythonOperator(task_id='load_dim_territory', python_callable=load_dim_territory_scd1)
    
    # Stage 4: Load Fact
    load_sales = PythonOperator(task_id='load_fact_sales', python_callable=load_fact_sales)
    
    # Dependencies
    truncate >> extract
    extract >> init_dw
    init_dw >> [load_date, load_prod, load_cust, load_terr]
    [load_date, load_prod, load_cust, load_terr] >> load_sales
