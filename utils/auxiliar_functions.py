import csv
import os
import pandas as pd
from config.config_loader import load_config_file
from validation.seach_values import execute_search
from utils.retrieve_participants_ids import read_all_dataframes


IMMERSE_GENERAL_REPOSITORY_PATH = load_config_file('immerse_general_repository', 'general_repository')
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
    dataframes, filenames = read_all_dataframes(directory, system)
    rows = []
    for df, file in zip(dataframes, filenames):
        if not df.empty:
            for col in df.columns:
                rows.append({
                    f'variables_{system}': col,
                    'filename': file.replace(".csv", "")
                })

    codebook_df = pd.DataFrame(rows)
    excel_filepath = os.path.join(IMMERSE_GENERAL_REPOSITORY_PATH, f"codebook_{system}.xlsx")
    codebook_df.to_excel(excel_filepath, index=False)
    csv_filepath = os.path.join(IMMERSE_GENERAL_REPOSITORY_PATH, f"codebook_{system}.csv")
    codebook_df.to_csv(csv_filepath, index=False, sep=';', quoting=csv.QUOTE_NONNUMERIC)
    print(f"Codebook size: {len(codebook)} , successfully exported as: {excel_filepath}!")


def merge_dataframes(f1, f2, system):
    print("Merging dfs..")
    print(f1, "\n", f2)
    df1 = pd.read_csv(f1, sep=";") if f1.endswith(".csv") else pd.read_excel(f1, engine='openpyxl')
    df2 = pd.read_csv(f2, sep=";") if f2.endswith(".csv") else pd.read_excel(f2, engine='openpyxl')

    df1.info()
    df2.info()

    on_cols = (
        ['participant_identifier', 'participant_number', 'VisitCode', 'SiteCode']
        if "movisens_esm" in system
        else 'participant_identifier'
    )
    how = "outer" if "movisens_esm" in system else "left"

    merged_df = pd.merge(df2, df1, on=on_cols, how=how)
    filename = f"{system}_summary_ids.csv"
    merged_df.to_csv(filename, index=False, sep=";")


def extract_unique_identifiers(filepath, column_name):
    print("\nPreparing summary...")
    current_df = pd.read_excel(filepath, engine='openpyxl') if filepath.endswith(".xlsx") else pd.read_csv(filepath, sep=";", quotechar='"')
    if column_name in current_df.columns:
        unique_values_df = current_df[column_name].drop_duplicates().dropna()
        output_filename = f'extract_unique_values_{column_name}.xlsx'
        output_file = os.path.join(os.path.dirname(filepath), output_filename)
        unique_values_df.to_excel(output_file, index=False)
        # unique_values_df.to_csv(output_file, sep=';', index=False, quoting=csv.QUOTE_ALL)
        print(f"Exported {len(unique_values_df)} from '{column_name}' unique IDs.")


def concat_files(directory, system):
    concatenated_files = []
    dataframes, filenames = read_all_dataframes(directory, system)
    for dataframe, filename in zip(dataframes, filenames):
        if filename in esm_files_to_exclude and "Fidelity" in filename:
            concatenated_files.append(dataframe)

    concatenated_df = pd.concat(concatenated_files)
    concatenated_df.drop_duplicates()
    new_filepath = os.path.join(directory, system)
    concatenated_df.to_csv(os.path.join(new_filepath, f'{system}_merged.csv'), sep=";", index=False)
    print(f"Exported {len(concatenated_df)}")


# concat_files(directory=ID_CLEAN_IMMERSE_PATH, system='movisens_fidelity')
# create_codebook(ID_CLEAN_IMMERSE_PATH, 'maganamed')
