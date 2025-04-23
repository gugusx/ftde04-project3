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

# Create Data Mart with Repurchase
def create_user_repurchase_data_mart(df):
    """Transform data into a Data Mart with user repurchase"""

    df['order_count'] = df.groupby('user_id')['order_id'].transform('count')
    df['avg_order_count_per_user'] = df.groupby('user_id')['order_count'].transform('mean')
    df_unique = df[['user_id', 'user_name', 'order_count', 'avg_order_count_per_user']].drop_duplicates()

    # Return the resulting DataFrame with user_id, user_name, and avg_order_count_per_user
    return df_unique[['user_id', 'user_name', 'avg_order_count_per_user']]


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
        
def run_etl_mart_UserRepurchase():
    """Run the ETL process for creating Data Mart"""
    try:
        # Define the queries to extract data
        query = """
            SELECT o.order_id, o.user_id, u.user_first_name || ' ' || u.user_last_name as user_name 
            FROM fact_orders o
            INNER JOIN dim_user u
            ON o.user_id = u.user_id;
        """
        
        # Extract data
        df = extract(query, warehouse_conn_string)
        user_repurchase_data_mart = create_user_repurchase_data_mart(df)
        load_to_data_mart(user_repurchase_data_mart, 'mart_user_repurchase', warehouse_conn_string)

        logging.info("Process Completed Successfully!")
    
    except Exception as e:
        logging.error(f"process failed: {e}")

# %%
if __name__ == "__main__":
    run_etl_mart_UserRepurchase()

# %%



