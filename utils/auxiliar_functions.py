import csv
import os
import pandas as pd
from config.config_loader import load_config_file
from validation.seach_values import execute_search
from utils.retrieve_participants_ids import read_all_dataframes

IMMERSE_GENERAL_REPOSITORY_PATH = load_config_file('immerse_master_repository', 'general_repository')
IMMERSE_ORIGINAL_SOURCE_PATH = load_config_file('original_source', 'immerse')
ID_CLEAN_IMMERSE_PATH = load_config_file('updated_source', 'immerse_clean')

esm_files_to_exclude = ["codebook.xlsx", "Fidelity_BE.xlsx", "Fidelity_c_UK.xlsx", "Fidelity_GE.xlsx",
                    "Fidelity_SK.xlsx", "Fidelity_UK.xlsx", "IMMERSE_Fidelity_SK_Kosice.xlsx", "Sensing.xlsx"]

files_to_filter = [
    "Informed-consent.csv",
    "Service-characteristics-(Teamleads).csv",
    "Service-characteristics.csv",
    "ORCA.csv"
]


def search_value_from_db():
    input_value = ['Screening']  # TODO: Change these values for real IDs or value to search.
    execute_search(input_value)


def get_filenames_per_system(original_directory):
    filenames_per_system = {}
    files = []

    for root, dirs, files in os.walk(original_directory):
        for file in files:
            if file in esm_files_to_exclude:
                continue
            if file.endswith(".xlsx") and file.startswith("_IMMERSE"):
                old_path = os.path.join(root, file)
                new_filename = f"{file.replace('_IMMERSE', 'IMMERSE')}"
                new_path = os.path.join(root, new_filename)
                os.rename(old_path, new_path)
                print(f"Renamed {file} to {new_filename}")
                # os.remove(os.path.join(root, file))


def filter_only_participants(df, id_column):
    filtered_df = df[
        ~df[id_column].str.contains(r'[_-]C[_-]', case=False, na=False) &
        ~df[id_column].str.contains(r'[_-]A[_-]', case=False, na=False)
        ]
    return filtered_df


# In the new processed data from GH, IDs from Clinicians and Admin are combined in some files.
def export_clinicians_and_participants(directory):
    for file in files_to_filter:
        filepath = os.path.join(directory, file)
        print("filepath = ", filepath)
        df = pd.read_csv(filepath, sep=";")
        file_to_export = filter_only_participants(df, "participant_identifier")
        file_to_export.to_csv(file, sep=";", index=False)
        print(f"Exported {file}")


def convert_file_to_csv(filepath):
    filename = os.path.basename(filepath)
    print(f"Converting {filename} to CSV")
    df = pd.read_excel(filepath)
    df.to_csv(filename.replace(".xlsx", ".csv"), sep=";", index=False)


def create_codebook(directory, system):
    codebook = set()

    print(f"Creating codebook for {system}...")
    dataframes = read_all_dataframes(directory, system)
    for dataframe in dataframes:
        print(dataframe.columns)
        if not dataframe.empty:
            codebook.update(dataframe.columns)

    codebook_df = pd.DataFrame({f'variables_{system}': sorted(codebook)})
    excel_filepath = os.path.join(IMMERSE_GENERAL_REPOSITORY_PATH, f"codebook_{system}.xlsx")
    codebook_df.to_excel(excel_filepath, index=False)
    # TODO: Uncomment if CSV file is needed.
    # csv_filepath = os.path.join(IMMERSE_GENERAL_REPOSITORY_PATH, f"codebook_{system}.csv")
    # codebook_df.to_csv(csv_filepath, index=False, sep=';', quoting=csv.QUOTE_NONNUMERIC)
    print(f"Codebook size: {len(codebook)} , successfully exported as: {excel_filepath}!")


# create_codebook(IMMERSE_ORIGINAL_SOURCE_PATH, 'dmmh')
