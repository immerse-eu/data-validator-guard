import sqlite3
import yaml
import os
from config.config_loader import load_config_file

DB_PATH = load_config_file('researchDB', 'db_path')
NEW_DB_PATH = load_config_file('researchDB', 'cleaned_db')

def search_db_tables(db_path, search_value):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    found_in_tables = []

    for (table_name,) in tables:
        try:
            cursor.execute(f'PRAGMA table_info("{table_name}");')
            columns = [row[1] for row in cursor.fetchall()]

            for column in columns:
                try:
                    # Only check if any row exists, return immediately for performance
                    query = f'SELECT 1 FROM "{table_name}" WHERE "{column}" LIKE ? LIMIT 1;'
                    cursor.execute(query, (f"%{search_value}%",))
                    if cursor.fetchone():
                        found_in_tables.append(table_name)
                        break
                except Exception:
                    continue
        except Exception:
            continue

    conn.close()

    if found_in_tables:
        print(f"Value found in the following table(s): {', '.join(found_in_tables)}")
    else:
        print(f'"{search_value}" not found in any table.')


def execute_search(search_values_list):
    for search_value in search_values_list:
        print("searching value:", search_value)
        search_db_tables(NEW_DB_PATH, search_value)  # Change to DB_PATH when necessary
