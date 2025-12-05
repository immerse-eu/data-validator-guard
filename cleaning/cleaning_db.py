import os
import sqlite3
import pandas as pd
from config.config_loader import load_config_file

FIXES_FILE_PATH = load_config_file('reports', 'fixes')
CHANGES_FILE_PATH = load_config_file('reports', 'changes')
DB_PATH = load_config_file('researchDB', 'db_path')
NEW_DB_PATH = load_config_file('researchDB', 'clean_db')


def get_all_tables(path_db):
    conn = sqlite3.connect(path_db)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    table_names = [row[0] for row in cursor.fetchall()]
    return table_names


def get_master_file(fixed_filepath):
    for file in os.listdir(fixed_filepath):
        if 'Kind' in file and file.endswith('.csv'):
            return pd.read_csv(os.path.join(fixed_filepath, file))


def has_column(conn, table_name, column_name):
    cursor = conn.cursor()
    cursor.execute(f'PRAGMA table_info("{table_name}")')
    columns = [row[1] for row in cursor.fetchall()]
    return column_name in columns


def apply_changes(conn, table, change_type, row):
    cursor = conn.cursor()
    participant_identifier = row['participant_identifier']
    new_value = row['Expected_value']

    # if change_type == 'changes_df_by_id':
    #     column = 'participant_identifier'
    #     updated_columns = []
    #
    #     if has_column(conn, table, column) and has_column(conn, table, 'participant_identifier'):
    #         cursor.execute(f'UPDATE "{table}" SET {column} = ? WHERE participant_identifier = ?',
    #                        (new_value, participant_identifier))
    #         updated_columns.append(column)
    #
    #     if updated_columns:
    #         print(f"Updated {table}: columns by changes_df_by_id {', '.join(updated_columns)}.")

    if change_type == 'changes_df_by_center_name':
        column = 'center_name'
        updated_columns = []

        if has_column(conn, table, column) and has_column(conn, table, 'participant_identifier'):
            cursor.execute(f'UPDATE "{table}" SET {column} = ? WHERE participant_identifier = ?',
                           (new_value, participant_identifier))
            updated_columns.append(column)

            print(f"{cursor.rowcount} row(s) updated in '{table}' for participant_identifier = {participant_identifier}")
        else:
            print(f"Required columns not found in table {table}")

    if change_type == 'changes_df_by_center_name':
        column = 'center_name'
        updated_columns = []

        if has_column(conn, table, column) and has_column(conn, table, 'participant_identifier'):
            cursor.execute(f'UPDATE "{table}" SET {column} = ? WHERE participant_identifier = ?',
                           (new_value, participant_identifier))
            updated_columns.append(column)

        if updated_columns:
            print(f"Updated {table}: columns by changes_df_by_center_name {', '.join(updated_columns)}.")

    if change_type == 'changes_df_by_site':
        updated_columns = []
        for column in ['Site', 'SiteCode']:
            if has_column(conn, table, column) and has_column(conn, table, 'participant_identifier'):
                cursor.execute(
                    f'UPDATE "{table}" SET {column} = ? WHERE participant_identifier = ?',
                    (new_value, participant_identifier)
                )
                updated_columns.append(column)
        if updated_columns:
            print(f"Updated {table}: columns {', '.join(updated_columns)}.")
        else:
            print(f"Skipping {table}: Neither 'Site' nor 'SiteCode' found.")


def cleaning_db(path_db, system):
    if system == 'maganamed':   # TODO: Verify and complete changes by system
        changes_df = get_master_file(CHANGES_FILE_PATH)
        changes_df = changes_df[["participant_identifier", "site_validation_result", "Expected_value"]]

        #changes_df_by_id = changes_df[changes_df["id_validation_result"] == "ID-mismatch"]
        changes_df_by_center_name = changes_df[changes_df["Expected_value"] == "Camhs"]
        filter_by_int_values = pd.to_numeric(changes_df["Expected_value"], errors='coerce').notna()
        changes_df_by_site = changes_df[filter_by_int_values]

        change_sets = [
            #("changes_df_by_id", changes_df_by_id),
            ("changes_df_by_center_name", changes_df_by_center_name),
            ("changes_df_by_site", changes_df_by_site)
        ]

        conn = sqlite3.connect(path_db)

        for change_type, df in change_sets:
            for table in get_all_tables(path_db):
                for _, row in df.iterrows():
                    apply_changes(conn, table, change_type, row)
        conn.commit()
        conn.close()


