# %%
import logging
import pandas as pd
import sys

from urllib.parse import quote_plus
from sqlalchemy import create_engine

from config import warehouse_conn_string

import warnings
warnings.filterwarnings('ignore')

sys.path.append("/opt/airflow/script_etl")

# %%
# Logging setup
logging.basicConfig(level=logging.INFO)

def db_connection(conn_params):
    """Create a connection engine to the database"""
    conn_str = f"postgresql://{conn_params['user']}:{quote_plus(conn_params['password'])}@{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
    engine = create_engine(conn_str)
    return engine.connect()

# Extract data from source using SQL query
def extract(query, conn_params):
    """Extract data from PostgreSQL source table using a SQL query"""
    try:
        logging.info(f"Extracting data using query: {query}")
        with db_connection(conn_params) as conn:
            df = pd.read_sql(query, conn)
        logging.info("Data extraction successful")
        return df
    except Exception as e:
        logging.error(f"Error extracting data: {e}")
        raise

# Create Data Mart with Purchase Period
def create_user_purchase_data_mart(df):
    """Transform data into a Data Mart with user purchase period, repurchase count, and revenue per period"""
    
    # 1. Calculate User Purchase Period (Days between user registration and first purchase)
    df['first_purchase_date'] = df.groupby('user_id')['order_date'].transform('min')
    df['purchase_period'] = (df['first_purchase_date'] - df['user_join'])
    df = df.drop_duplicates('user_id').sort_values('user_id')
    df['purchase_period_value'] = df['purchase_period'].astype(str).str[0]

    return df

# Load the transformed data into Data Mart
def load_to_data_mart(df, table_name, conn_params):
    """Load the transformed data into Data Mart"""
    try:
        logging.info(f"Loading data into {table_name}...")
        with db_connection(conn_params) as conn:
            df.to_sql(table_name, conn, if_exists='replace', index=False)
        logging.info(f"Data successfully loaded into {table_name}")
    except Exception as e:
        logging.error(f"Error loading data into {table_name}: {e}")
        raise
        
def run_etl_mart_UserPurchase():
    """Run the ETL process for creating Data Mart"""
    try:
        # Define the queries to extract data
        query = """
            SELECT  o.order_id, 
                    o.order_date, 
                    o.user_id, 
                    u.user_first_name || ' ' || u.user_last_name AS user_name, 
                    o.order_total, u.user_join
            FROM fact_orders o
            JOIN dim_user u ON o.user_id = u.user_id
        """
        
        # Extract data
        df = extract(query, warehouse_conn_string)
        user_purchase_data_mart = create_user_purchase_data_mart(df)
        load_to_data_mart(user_purchase_data_mart, 'mart_user_purchase', warehouse_conn_string)

        logging.info("Process Completed Successfully!")
    
    except Exception as e:
        logging.error(f"process failed: {e}")

# %%
if __name__ == "__main__":
    run_etl_mart_UserPurchase()

# %%



