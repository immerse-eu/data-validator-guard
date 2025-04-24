import sqlite3
import yaml


def read_config(category, path):
    with open("./assets/config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
        return config[category][path]


def search_db(db_path, search_value):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    results = []

    for (table_name,) in tables:
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = [row[1] for row in cursor.fetchall()]

        for column in columns:
            try:
                query = f"SELECT * FROM {table_name} WHERE {column} LIKE ?"
                cursor.execute(query, (f"%{search_value}%",))
                rows = cursor.fetchall()
                if rows:
                    results.append({
                        "table": table_name,
                        "column": column,
                        "matches": rows
                    })
            except Exception as e:
                continue

    conn.close()
    return results


def execute_search(search_values_list):
    db_path = read_config("researchDB", "base_path")
    for search_value in search_values_list:
        matches = search_db(db_path, search_value)

    for match in matches:
        print(f"Found in table '{match['table']}' column '{match['column']}':")
        for row in match['matches']:
            print(row)
