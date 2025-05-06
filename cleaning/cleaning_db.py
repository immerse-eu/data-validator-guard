import os
import sqlite3

import pandas as pd
from config.config_loader import load_config_file

FIXES_FILE_PATH = load_config_file('reports','fixes')
DB_PATH = load_config_file('researchDB','db_path')
# OUTPUT_CLEANING_DB_PATH = load_config_file('researchDB','cleaning_process')

def get_all_tables(path_db):
    conn = sqlite3.connect(path_db)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    table_names = [row[0] for row in cursor.fetchall()]
    return table_names

def get_master_file(fixed_filepath):
    for file in os.listdir(fixed_filepath):

        if 'Kind-of-participant'in file and file.endswith('.csv'):
            return pd.read_csv(os.path.join(fixed_filepath, file))


def cleaning_db():
    new_kind_of_participant = get_master_file(FIXES_FILE_PATH)
    list_tables = get_all_tables(DB_PATH)
    for i, table in enumerate(list_tables, start=1):
        print(i, table)

cleaning_db()


