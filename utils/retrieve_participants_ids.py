import os
import pandas as pd
from pathlib import Path

from database.db import connect_and_fetch_table
from config.config_loader import load_config_file

NEW_DB_PATH = load_config_file('researchDB', 'cleaned_db')
DB_CATALOGUE_PATH = load_config_file('researchDB', 'db_catalogue')
IMMERSE_CLEANING_SOURCE = load_config_file('updated_source', 'immerse_clean')

files_to_exclude = ["codebook.xlsx", "Fidelity_BE.xlsx", "Fidelity_c_UK.xlsx", "Fidelity_GE.xlsx",
                    "Fidelity_SK.xlsx", "Fidelity_UK.xlsx", "IMMERSE_Fidelity_SK_Kosice.xlsx", "Sensing.xlsx"]


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


def read_all_dataframes(original_directory, immerse_system):
    current_sub_directory = None
    filenames = []
    dataframes = []

    # -- Get interested directory
    for root, dirs, files in os.walk(original_directory):
        if immerse_system in dirs:
            current_sub_directory = os.path.join(root, immerse_system)

    csv_files = list(Path(current_sub_directory).rglob("*.csv"))
    excel_files = list(Path(current_sub_directory).rglob("*.xlsx"))

    if csv_files:
        for csv in csv_files:
            separator = detect_separator(csv)  # Verify
            filenames.append(csv.name)
            print("CSV file:", csv.name)
            df = pd.read_csv(csv, sep=separator, low_memory=False)
            dataframes.append(df)

    elif excel_files:
        for excel in excel_files:
            if "movisens_esm" in immerse_system and excel.name in files_to_exclude:
                continue
            filenames.append(excel.name)
            df = pd.read_excel(excel)
            if isinstance(df, pd.DataFrame):
                dataframes.append(df)
    else:
        pass

    return dataframes, filenames


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

    if 'participant_id' in df.columns:
        print("participant_id type")
        ids_df = df['participant_id']
        unique = ids_df.drop_duplicates().dropna()
        unique_ids.update(unique)

        '''
        # Extended id collection from Movisens ESM. TODO: Uncomment when necessary.
        # Get the first 5 columns (participant id., number, country, visit and site code).       
        '''
        # unique = df.iloc[:, :5].drop_duplicates()
        # print("unique ids: ", unique, len(unique))
        # unique_ids.update(unique.values.flatten())
        # return {tuple(row) for row in unique.itertuples(index=False)}

    elif 'study_id' in df.columns:
        print("study_id type")
        ids_df = df['study_id']
        unique_ids.update(ids_df.dropna().unique())

    elif 'id' in df.columns:
        print("id type")
        ids_df = df[['Participant', 'id']]
        unique = ids_df.drop_duplicates()
        unique_ids.update(set(zip(unique['Participant'], unique['id'])))
        # TODO: ---> Uncomment just for fidelity files!!
        # ids_df = df[['Participant', 'patient_id', 'id']]
        # unique = ids_df.drop_duplicates()
        # unique_ids.update(set(zip(unique['Participant'], unique['patient_id'], unique['id'])))
    elif 'Participant' in df.columns:
        ids_df = df['Participant']
        unique_ids.update(ids_df.dropna().unique())

    else:
        print("No participant id recognised")

    return unique_ids


def get_unique_participant_identifier_per_system(system, source_type):
    unique_participant_identifiers = set()

    print(f'Getting participant ids from {system}...')
    if source_type == 'db':
        filtered_tablename_df = read_db_catalogue(DB_CATALOGUE_PATH, system)
        for tablename in filtered_tablename_df:
            print("Table name: ", tablename)
            if source_type == 'database':
                sql_df = connect_and_fetch_table(tablename)
                if sql_df is not None:
                    participant_identifiers = export_ids_per_table(sql_df)
                    unique_participant_identifiers.update(participant_identifiers)

    if source_type == 'files':
        dataframes, filenames = read_all_dataframes(original_directory=IMMERSE_CLEANING_SOURCE, immerse_system=system)
        for df in dataframes:
            unique_identifiers = export_tricky_ids(df)
            unique_participant_identifiers.update(unique_identifiers)

    print("Unique participant identifiers per system: ", len(unique_participant_identifiers))
    columns = ['participant_identifier', 'participant_number', 'Country', 'VisitCode', 'SiteCode']  # MovisensESM only
    if columns in unique_participant_identifiers:
        rows = list(unique_participant_identifiers)   # TODO: Verify functionally. if not: rollback to last commit.
        unique_participants_df = pd.DataFrame(rows, columns=columns)
    else:
        unique_participants_df = pd.DataFrame({f'participant_identifier': sorted(unique_participant_identifiers)})
    output_filename = f'extracted_ids_{system}.xlsx'
    output_file = os.path.join(os.path.dirname(DB_CATALOGUE_PATH), output_filename)
    unique_participants_df.to_excel(output_file, index=False)
    print("File exported in:", output_file)


# For source type, there are two options: "database" or "files"
# get_unique_participant_identifier_per_system(system='dmmh', source_type='files')
