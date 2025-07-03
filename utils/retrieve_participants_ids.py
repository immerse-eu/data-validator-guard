# TODO: This function will gather only participant_identifier (IDs) from DB  to control "valid" or "invalid" ids.
import os

import pandas as pd

from database.db import connect_and_fetch_table
from config.config_loader import load_config_file

NEW_DB_PATH = load_config_file('researchDB', 'cleaned_db')
DB_CATALOGUE_PATH = load_config_file('researchDB', 'db_catalogue')


def read_db_catalogue(filepath, source):
    df = pd.read_excel(filepath)
    df['Source'] = df['Source'].str.lower()
    filter_df = df[df['Source'] == source]
    return filter_df['Tablename']


def export_ids_per_table(df):
    unique_ids = set()
    ids_df = df.iloc[:, 0]
    unique_ids.update(ids_df.dropna().unique())
    return unique_ids


def get_unique_participant_identifier_per_system(system):
    unique_participant_identifiers = set()
    filtered_tablename_df = read_db_catalogue(DB_CATALOGUE_PATH, system)
    for tablename in filtered_tablename_df:
        print("Table name: ", tablename)
        sql_df = connect_and_fetch_table(tablename)
        participant_identifiers = export_ids_per_table(sql_df)
        unique_participant_identifiers.update(participant_identifiers)

    print("Unique participant identifiers per system: ", len(unique_participant_identifiers))
    sorted_unique_participants = sorted(unique_participant_identifiers)
    unique_participants_df = pd.DataFrame(sorted_unique_participants, columns=['participant_identifier'])
    output_filename = f'unique_identifiers_per_participant_from_{system}.csv'
    output_file = os.path.join(os.path.dirname(DB_CATALOGUE_PATH), output_filename)
    unique_participants_df.to_csv(output_file, sep=';', index=False)
    print("file exported")


get_unique_participant_identifier_per_system('maganamed')
