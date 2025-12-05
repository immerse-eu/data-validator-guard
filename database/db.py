import os
import sqlite3
import csv
import pandas as pd
import datetime
from sqlite3 import DatabaseError, Error
from config.config_loader import load_config_file
from pandas.errors import DatabaseError


TEMPORAL_SQL_DB_PATH = load_config_file('researchDB', 'db_path')


def connect_and_fetch_table(table_name):
    sql_connection = sqlite3.connect(TEMPORAL_SQL_DB_PATH)

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


def create_connection(db_file):
    connection = None
    try:
        connection = sqlite3.connect(db_file)
        print("version:", sqlite3.sqlite_version)
    except Error as e:
        print(e)
    return connection


def import_data_into_sql_lite(conn, filename, csv_data):
    cursor = conn.cursor()
    csv_data.to_sql(filename, conn, if_exists='replace', index=False)
    print(f"Importing {filename}...")
    cursor.close()


def detect_delimiter(filepath):
    with open(filepath, "r", encoding="utf-8") as file:
        sample = file.readline()
        try:
            delimiter = csv.Sniffer().sniff(sample, delimiters=";,")
        except csv.Error:
            return ";"
        return delimiter.delimiter


def retrieve_input_files(path, connect):
    for root, dirs, files in os.walk(path):
        for filename in files:
            filepath = os.path.join(root, filename)
            if filename.endswith('.csv'):
                delimiter = detect_delimiter(filepath)
                csv_data = pd.read_csv(filepath, delimiter=delimiter, engine='python')
                filename = filename.replace(".csv", "")
                import_data_into_sql_lite(connect, filename, csv_data)


def create_database(sql_lite_database_directory, database_name):

    immerse_directory = {
        'maganamed_path': load_config_file('immerse_cleaned_ids', 'maganamed'),
        'movisens_esm_path': load_config_file('immerse_cleaned_ids', 'movisens_esm'),
        'movisens_sensing_path': load_config_file('immerse_cleaned_ids', 'movisens_sensing'),
        'movisens_fidelity_path': load_config_file('immerse_cleaned_ids', 'redcap_id_summary'),
        'dmmh_app_path': load_config_file('immerse_cleaned_ids', 'dmmh_momentapp'),
        'dmmh_summary_path': load_config_file('immerse_preprocessed', 'dmmh_logins'),
        'redcap_summary_path': load_config_file('immerse_preprocessed', 'redcap_master_ids'),
    }

    now = datetime.datetime.now()
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
    complete_db_filename = f"{database_name}_{timestamp}.db"

    sql_lite_database_path = os.path.join(sql_lite_database_directory, f'{complete_db_filename}')
    print("database path: ", sql_lite_database_path)

    connect = create_connection(sql_lite_database_path)
    if connect is not None:
        print(f"Successful connection to '{complete_db_filename}' database")

    key_system = list(immerse_directory.keys())
    print("IMMERSE directories: ", key_system)

    for key_system, data_directory in immerse_directory.items():
        print("Obtaining data from " + key_system)
        retrieve_input_files(data_directory, connect)

    if connect:
        connect.commit()
        connect.close()


def clone_database(original_path, new_path, new_name):
    print("cloning database...")
    os.makedirs(new_path, exist_ok=True)
    now = datetime.datetime.now()
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
    complete_db_filename = f"{new_name}_{timestamp}.db"
    new_path = os.path.join(new_path, complete_db_filename)

    if os.path.exists(new_path):
        print(f"Database already exists at: {new_path}, overwriting.")
    with sqlite3.connect(original_path) as source_conn:
        with sqlite3.connect(new_path) as dest_conn:
            source_conn.backup(dest_conn)
            print(f"Database cloned successfully to {new_path}")
