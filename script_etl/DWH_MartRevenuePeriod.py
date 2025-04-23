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

# Create Data Mart Revenue Period
def create_revenue_period_data_mart(df):
    """Transform data into a Data Mart with Revenue Period"""

    revenue_df = df.groupby('order_date').agg(
        daily_revenue=('order_total', 'sum')).reset_index()
    return revenue_df


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
        
def run_etl_mart_RevenuePeriod():
    """Run the ETL process for creating Data Mart"""
    try:
        # Define the queries to extract data
        query = """
            SELECT order_date, order_total
            FROM fact_orders;
        """
        
        # Extract data
        df = extract(query, warehouse_conn_string)
        revenue_period_data_mart = create_revenue_period_data_mart(df)
        load_to_data_mart(revenue_period_data_mart, 'mart_revenue_period', warehouse_conn_string)

        logging.info("Process Completed Successfully!")
    
    except Exception as e:
        logging.error(f"process failed: {e}")

# %%
if __name__ == "__main__":
    run_etl_mart_RevenuePeriod()

# %%



