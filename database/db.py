import sqlite3
import pandas as pd
from config.config_loader import load_config_file

DB_PATH = load_config_file('researchDB', 'db_path')
NEW_DB_PATH = load_config_file('researchDB', 'cleaned_db')


def connect_and_fetch_table(table_name):
    sql_connection = sqlite3.connect(NEW_DB_PATH)
    try:
        query = f"SELECT * FROM `{table_name}`"
        df = pd.read_sql_query(query, sql_connection)
    finally:
        sql_connection.close()
    return df
