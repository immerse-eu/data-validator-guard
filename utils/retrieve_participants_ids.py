# TODO: This function will gather only participant_identifier (IDs) from DB  to control "valid" or "invalid" ids.
import os
import pandas as pd

from database.db import connect_and_fetch_table
from config.config_loader import load_config_file

NEW_DB_PATH = load_config_file('researchDB', 'cleaned_db')
DB_CATALOGUE_PATH = load_config_file('researchDB', 'db_catalogue')
IMMERSE_CLEANING_SOURCE = load_config_file('updated_source', 'immerse_clean')


def detect_separator(filepath):
    with open(filepath, 'r',  encoding='utf-8', errors='ignore') as f:
        first_line = f.readline()

    for delimiter in [',', ';']:
        if delimiter in first_line:
            return delimiter


def read_dataframe(original_directory, file, immerse_system):
    for root, dirs, files in os.walk(original_directory):
        if immerse_system in dirs:
            sub_folder_path = os.path.join(root, immerse_system)
            for folder, _, files in os.walk(sub_folder_path):
                # print("subfolder", sub_folder_path)
                for filename in files:
                    if filename.endswith(".xlsx") or filename.endswith(".csv"):
                        filepath = os.path.join(folder, filename)
                        separator = detect_separator(filepath)
                        # print("Filepath", filepath)
                        try:
                            print("Current filename", filename)
                            current_df = pd.read_excel(filepath, engine='openpyxl') if filename.endswith(".xlsx") \
                                else pd.read_csv(filepath, sep=separator, encoding='utf-8', low_memory=False)
                            return current_df

                        except Exception as e:
                            print(f"Unexpected error in  {filename}", e)


def read_db_catalogue(filepath, source):
    if filepath:
        df = pd.read_excel(filepath)
        df['Source'] = df['Source'].str.lower()
        filter_df = df[df['Source'] == source]
        return filter_df['Tablename']


def export_ids_per_table(df):
    unique_ids = set()
    ids_df = df.iloc[:, 0]
    unique_ids.update(ids_df.dropna().unique())
    return unique_ids


def export_tricky_ids(df):
    unique_ids = set()
    unique_ids_and_participant_number = set()
    if 'participant_id' in df.columns:
        print(df['study_id'])
        ids_df = df['participant_id']
        unique_ids.update(ids_df.dropna().unique())
        return unique_ids

    if 'study_id' in df.columns:
        print(df['study_id'])
        ids_df = df['study_id']
        unique_ids.update(ids_df.dropna().unique())
        return unique_ids

    if 'id' in df.columns:
        ids_df = df['id']  # TODO: It needs participant to identify these which do not have participant ID
        unique_ids.update(ids_df.dropna().unique())
        print(unique_ids, len(unique_ids))
        return unique_ids


def get_unique_participant_identifier_per_system(system, source_type):
    unique_participant_identifiers = set()
    filtered_tablename_df = read_db_catalogue(DB_CATALOGUE_PATH, system)
    for tablename in filtered_tablename_df:
        print("Table name: ", tablename)
        if source_type == 'database':
            sql_df = connect_and_fetch_table(tablename)
            if sql_df is not None:
                participant_identifiers = export_ids_per_table(sql_df)
                unique_participant_identifiers.update(participant_identifiers)
        if source_type == 'files':
            df = read_dataframe(original_directory=IMMERSE_CLEANING_SOURCE, file=tablename, immerse_system=system)
            if 'movisens' in system:
                participant_identifiers = export_tricky_ids(df)
                unique_participant_identifiers.update(participant_identifiers)
            else:
                participant_identifiers = export_ids_per_table(df)
                unique_participant_identifiers.update(participant_identifiers)

    print("Unique participant identifiers per system: ", len(unique_participant_identifiers))
    sorted_unique_participants = sorted(unique_participant_identifiers)
    unique_participants_df = pd.DataFrame(sorted_unique_participants, columns=['participant_identifier'])
    output_filename = f'new_unique_identifiers_per_participant_from_{system}.csv'
    output_file = os.path.join(os.path.dirname(DB_CATALOGUE_PATH), output_filename)
    unique_participants_df.to_csv(output_file, sep=';', index=False)
    print("File exported in:", output_file)


# For source type, there are two options: "database" or "files"
get_unique_participant_identifier_per_system(system='movisens_esm', source_type='files')
