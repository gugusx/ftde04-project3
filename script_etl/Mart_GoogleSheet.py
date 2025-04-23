# %%
import json
import gspread
import logging
import traceback
import sys

import pandas as pd

from urllib.parse import quote_plus
from sqlalchemy import create_engine

from config import warehouse_conn_string
from oauth2client.service_account import ServiceAccountCredentials

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
        
# Load the data mart into Google Sheet
def load_to_google_sheet(df, worksheet_name):
    """Load the transformed data into the specified worksheet in the Google Sheet"""
    try:
        with open('/opt/airflow/key_looker/credential.json','rb') as file:
            key = json.load(file)

        scope = ['https://www.googleapis.com/auth/drive', 'https://spreadsheets.google.com/feeds']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(key, scope)
        client = gspread.authorize(creds)

        # Spreadsheet name
        # Share sheet to client-email in credential.json
        sheet = client.open('Project-3')

        # Select worksheet based on name
        export = sheet.worksheet(worksheet_name)

        # Update the entire worksheet with new data
        export.update([df.columns.values.tolist()] + df.astype(str).values.tolist())

    except Exception as e:
        logging.error(f"Error loading data into worksheet '{worksheet_name}': {e}")
        logging.error(traceback.format_exc())
        raise

        
def run_mart_gsheet():
    # Define your queries and target worksheet names
    tasks = [
        {
            "query": "SELECT * FROM mart_user_purchase;",
            "sheet_name": "Sheet1"
        },
        {
            "query": "SELECT * FROM mart_user_repurchase;",
            "sheet_name": "Sheet2"
        },
        {
            "query": "SELECT * FROM mart_revenue_period;",
            "sheet_name": "Sheet3"
        }
    ]
    
    for task in tasks:
        try:
            logging.info(f"Processing worksheet: {task['sheet_name']}")
            df = extract(task["query"], warehouse_conn_string)
            load_to_google_sheet(df, task["sheet_name"])
            logging.info(f"{task['sheet_name']} updated successfully!")
        except Exception as e:
            logging.error(f"Failed to process {task['sheet_name']}: {e}")


# %%
if __name__ == "__main__":
    run_mart_gsheet()


