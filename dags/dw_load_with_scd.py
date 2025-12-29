"""
Data Warehouse Loading DAG
Loads from Staging (stg) to Data Warehouse (dw) with SCD support
- DimProduct: SCD Type 2 (history tracking with valid dates)
- DimCustomer: SCD Type 1 (simple overwrite)
- DimTerritory: SCD Type 1 (simple overwrite)
- FactSales: Fact table with surrogate key lookups
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
from psycopg2.extras import RealDictCursor

# Load .env
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)
else:
    load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Local PostgreSQL 18 (DW database)
DW_POSTGRES_CONFIG = {
    "host": os.getenv("LOCAL_POSTGRES_HOST", "host.docker.internal"),
    "port": int(os.getenv("LOCAL_POSTGRES_PORT", "1223")),
    "dbname": os.getenv("LOCAL_POSTGRES_DB", "AdventureWorksOLTP"),
    "user": os.getenv("LOCAL_POSTGRES_USER", "postgres"),
    "password": os.getenv("LOCAL_POSTGRES_PASSWORD", "postgres")
}

# Docker postgres (intermediate - where extraction loads)
STAGING_POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "airflow"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow")
}

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def init_dw_schema(**context):
    """Initialize DW schema (only if it doesn't exist)."""
    try:
        conn = psycopg2.connect(**DW_POSTGRES_CONFIG)
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
        # Read and execute the schema initialization SQL
        schema_file = Path(__file__).parent / 'init_dw_schema.sql'
        if schema_file.exists():
            with open(schema_file, 'r') as f:
                sql = f.read()
            cur.execute(sql)
            conn.commit()
            logger.info("DW schema created successfully")
        else:
            logger.warning(f"Schema file not found: {schema_file}")
        
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error initializing DW schema: {e}")
        raise

def load_dim_date(**context):
    """Load/Update DimDate table with date range."""
    try:
        conn = psycopg2.connect(**DW_POSTGRES_CONFIG)
        cur = conn.cursor()
        
        # Generate dates for past and future years (2000-2030) to cover all sales data
        date_range = pd.date_range(start='2000-01-01', end='2030-12-31', freq='D')
        
        for date in date_range:
            sql = """
            INSERT INTO dw.dimdate (date, year, month, day, quarter, dayofweek, isweekend)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING;
            """
            cur.execute(sql, (
                date.date(),
                date.year,
                date.month,
                date.day,
                (date.month - 1) // 3 + 1,  # Quarter
                date.weekday(),  # Day of week
                date.weekday() >= 5  # Is weekend
            ))
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("DimDate loaded successfully")
    except Exception as e:
        logger.error(f"Error loading DimDate: {e}")
        raise

def load_dim_product_scd2(**context):
    """Load DimProduct with SCD Type 2 (history tracking).
    - If product doesn't exist: Insert as new row with validfrom = earliest date in sales
    - If product price changed: Expire old row, insert new row
    - If product exists with same price: Do nothing
    """
    try:
        # Read from staging
        stg_conn = psycopg2.connect(**STAGING_POSTGRES_CONFIG)
        stg_df = pd.read_sql("SELECT * FROM stg.dimproduct", stg_conn)
        
        # Get earliest order date to set validfrom properly
        earliest_date_df = pd.read_sql("SELECT MIN(orderdate) as min_date FROM stg.factsales", stg_conn)
        earliest_date = earliest_date_df['min_date'][0] if not earliest_date_df.empty else datetime.now().date()
        
        stg_conn.close()
        
        if stg_df.empty:
            logger.warning("No data in stg.dimproduct")
            return
        
        dw_conn = psycopg2.connect(**DW_POSTGRES_CONFIG)
        dw_cur = dw_conn.cursor()
        
        for _, row in stg_df.iterrows():
            product_id = row['productid']
            product_name = row['productname']
            product_number = row['productnumber']
            subcategory = row['subcategoryname']
            category = row['categoryname']
            list_price = row['listprice']
            
            # Check if product exists with current price
            dw_cur.execute("""
                SELECT productkey, listprice FROM dw.dimproduct
                WHERE productid = %s AND iscurrent = TRUE
            """, (product_id,))
            
            existing = dw_cur.fetchone()
            
            if existing is None:
                # NEW PRODUCT: Insert with validfrom = earliest sales date in data
                dw_cur.execute("""
                    INSERT INTO dw.dimproduct 
                    (productid, productname, productnumber, subcategoryname, categoryname, listprice, validfrom, validto, iscurrent)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NULL, TRUE)
                """, (product_id, product_name, product_number, subcategory, category, list_price, earliest_date))
                logger.info(f"Inserted new product: {product_id}")
            
            elif existing[1] != list_price:
                # PRICE CHANGED: Expire old, insert new (SCD Type 2)
                old_productkey = existing[0]
                
                # Expire old row
                dw_cur.execute("""
                    UPDATE dw.dimproduct
                    SET validto = %s, iscurrent = FALSE
                    WHERE productkey = %s
                """, (datetime.now().date(), old_productkey))
                
                # Insert new row
                dw_cur.execute("""
                    INSERT INTO dw.dimproduct
                    (productid, productname, productnumber, subcategoryname, categoryname, listprice, validfrom, validto, iscurrent)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NULL, TRUE)
                """, (product_id, product_name, product_number, subcategory, category, list_price, datetime.now().date()))
                logger.info(f"Updated product (SCD Type 2): {product_id}, new price: {list_price}")
            
            else:
                # NO CHANGE: Do nothing
                logger.debug(f"No change for product: {product_id}")
        
        dw_conn.commit()
        dw_cur.close()
        dw_conn.close()
        logger.info("DimProduct (SCD Type 2) loaded successfully")
        
    except Exception as e:
        logger.error(f"Error loading DimProduct: {e}")
        raise

def load_dim_customer_scd1(**context):
    """Load DimCustomer with SCD Type 1 (simple upsert).
    - If customer doesn't exist: Insert
    - If customer exists: Update name and type
    """
    try:
        # Read from staging
        stg_conn = psycopg2.connect(**STAGING_POSTGRES_CONFIG)
        stg_df = pd.read_sql("SELECT * FROM stg.dimcustomer", stg_conn)
        stg_conn.close()
        
        if stg_df.empty:
            logger.warning("No data in stg.dimcustomer")
            return
        
        dw_conn = psycopg2.connect(**DW_POSTGRES_CONFIG)
        dw_cur = dw_conn.cursor()
        
        for _, row in stg_df.iterrows():
            customer_id = row['customerid']
            account_number = row['accountnumber']
            customer_name = row['customername']
            customer_type = row['customertype']
            
            # Upsert: Insert or Update
            dw_cur.execute("""
                INSERT INTO dw.dimcustomer (customerid, accountnumber, customername, customertype, loaddate, updatedate)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (customerid) DO UPDATE SET
                    customername = EXCLUDED.customername,
                    customertype = EXCLUDED.customertype,
                    updatedate = CURRENT_TIMESTAMP
            """, (customer_id, account_number, customer_name, customer_type, datetime.now(), datetime.now()))
        
        dw_conn.commit()
        dw_cur.close()
        dw_conn.close()
        logger.info(f"DimCustomer (SCD Type 1) loaded: {len(stg_df)} rows")
        
    except Exception as e:
        logger.error(f"Error loading DimCustomer: {e}")
        raise

def load_dim_territory_scd1(**context):
    """Load DimTerritory with SCD Type 1 (simple upsert)."""
    try:
        # Read from staging
        stg_conn = psycopg2.connect(**STAGING_POSTGRES_CONFIG)
        stg_df = pd.read_sql("SELECT * FROM stg.dimterritory", stg_conn)
        stg_conn.close()
        
        if stg_df.empty:
            logger.warning("No data in stg.dimterritory")
            return
        
        dw_conn = psycopg2.connect(**DW_POSTGRES_CONFIG)
        dw_cur = dw_conn.cursor()
        
        for _, row in stg_df.iterrows():
            territory_id = row['territoryid']
            territory_name = row['territoryname']
            country_region_code = row['countryregioncode']
            
            dw_cur.execute("""
                INSERT INTO dw.dimterritory (territoryid, territoryname, countryregioncode, loaddate, updatedate)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (territoryid) DO UPDATE SET
                    territoryname = EXCLUDED.territoryname,
                    countryregioncode = EXCLUDED.countryregioncode,
                    updatedate = CURRENT_TIMESTAMP
            """, (territory_id, territory_name, country_region_code, datetime.now(), datetime.now()))
        
        dw_conn.commit()
        dw_cur.close()
        dw_conn.close()
        logger.info(f"DimTerritory (SCD Type 1) loaded: {len(stg_df)} rows")
        
    except Exception as e:
        logger.error(f"Error loading DimTerritory: {e}")
        raise

def load_fact_sales(**context):
    """Load FactSales with surrogate key lookups using SQL JOIN for speed.
    Joins stg.factsales to dimensions to get surrogate keys.
    """
    try:
        dw_conn = psycopg2.connect(**DW_POSTGRES_CONFIG)
        dw_cur = dw_conn.cursor()
        
        # Use SQL JOIN instead of row-by-row Python loop for MUCH better performance
        # This will handle 300K rows in seconds instead of minutes
        insert_sql = """
        INSERT INTO dw.factsales 
        (datekey, productkey, customerkey, territorykey, orderqty, unitprice, unitpricediscount, linetotal, loaddate)
        SELECT
            dd.datekey,
            dp.productkey,
            dc.customerkey,
            dt.territorykey,
            sf.orderqty,
            sf.unitprice,
            sf.unitpricediscount,
            sf.linetotal,
            CURRENT_TIMESTAMP
        FROM stg.factsales sf
        JOIN dw.dimdate dd ON sf.orderdate = dd.date
        JOIN dw.dimproduct dp ON sf.productid = dp.productid 
            AND dp.validfrom <= sf.orderdate 
            AND (dp.validto IS NULL OR dp.validto > sf.orderdate)
            AND dp.iscurrent = TRUE
        JOIN dw.dimcustomer dc ON sf.customerid = dc.customerid
        JOIN dw.dimterritory dt ON sf.territoryid = dt.territoryid
        """
        
        logger.info("Loading FactSales using SQL JOIN (optimized for 300K+ rows)...")
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
    dag_id='dw_load_with_scd',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=2)},
    max_active_runs=1,
) as dag:

    # Initialize DW schema
    init_schema = PythonOperator(
        task_id='init_dw_schema',
        python_callable=init_dw_schema
    )
    
    # Load dimensions
    load_date = PythonOperator(
        task_id='load_dim_date',
        python_callable=load_dim_date
    )
    
    load_product = PythonOperator(
        task_id='load_dim_product',
        python_callable=load_dim_product_scd2
    )
    
    load_customer = PythonOperator(
        task_id='load_dim_customer',
        python_callable=load_dim_customer_scd1
    )
    
    load_territory = PythonOperator(
        task_id='load_dim_territory',
        python_callable=load_dim_territory_scd1
    )
    
    # Load fact table
    load_sales = PythonOperator(
        task_id='load_fact_sales',
        python_callable=load_fact_sales
    )
    
    # Dependencies
    # Schema must be initialized first
    init_schema >> [load_date, load_product, load_customer, load_territory]
    
    # All dimensions must load before fact table
    [load_date, load_product, load_customer, load_territory] >> load_sales
