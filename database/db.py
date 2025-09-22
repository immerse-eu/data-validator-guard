import sqlite3
from sqlite3 import DatabaseError

import pandas as pd
from config.config_loader import load_config_file
from pandas.errors import DatabaseError


DB_PATH = load_config_file('researchDB', 'db_path')
NEW_DB_PATH = load_config_file('researchDB', 'cleaned_db')
# TEMPORAL_MAGANAMED_DB_PATH = ''


def connect_and_fetch_table(table_name):
    # sql_connection = sqlite3.connect(TEMPORAL_MAGANAMED_DB_PATH)
    sql_connection = sqlite3.connect(NEW_DB_PATH)

    try:
        query = f"SELECT * FROM `{table_name}`"
        df = pd.read_sql_query(query, sql_connection)
        return df
    except (DatabaseError, sqlite3.OperationalError) as e:
        if "no such table" in str(e):
            print(f"Table '{table_name}' not found. Skipping.")
            return None
        else:
            raise
    finally:
        sql_connection.close()
    return df
