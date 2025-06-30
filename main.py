import os
import pandas as pd

from cleaning.general_id_cleaning import DataCleaning, create_merged_esm_ids_rulebook
from config.config_loader import load_config_file
from validation.general_validation import DataValidator
from validation.maganamed_validation import VALID_SITE_CODES_AND_CENTER_NAMES
from maganamed import run_validation_maganamed, execute_corrections_maganamed

valid_center_names = VALID_SITE_CODES_AND_CENTER_NAMES.values()

DB_PATH = load_config_file('researchDB', 'db_path')
NEW_DB_PATH = load_config_file('researchDB', 'cleaned_db')

ISSUES_PATH = load_config_file('reports', 'issues')
CHANGES_PATH = load_config_file('reports', 'changes')
FIXES_PATH = load_config_file('reports', 'fixes')

IDS_REFERENCE_PATH = load_config_file('auxiliarFiles', 'ids_reference')
IDS_TO_VERIFY_PATH = load_config_file('auxiliarFiles', 'ids_to_verify')
IDS_MAGANAMED_RULEBOOK_PATH = load_config_file('auxiliarFiles', 'ids_reference_maganamed')
IDS_ESM_RULEBOOK_PATH = load_config_file('auxiliarFiles', 'ids_reference_esm')
ID_CLEAN_IMMERSE_PATH = load_config_file('updated_source', 'immerse_clean')


# General initial rule: ID validation
def general_validation_ids(df_control, rulebook, df_to_validate, file):
    print(f"\n\033[95m Validating IDS :\033[0m\n")
    general_validation = DataValidator(df_to_validate)
    general_validation.check_general_duplications(df_to_validate)
    general_validation.check_duplications_applying_normalisation('participant_identifier')
    general_validation.compare_ids_with_redcap_ids(df_control, id_column=0)
    general_validation.check_typos_in_ids(id_column=0)
    df_report = general_validation.report(ISSUES_PATH, file)

    # Cleaning process
    general_cleaning = DataCleaning(df_report)
    if "movisens_esm" in file:
        general_cleaning.prepare_ids_correction_from_esm(rulebook, CHANGES_PATH, file)
        general_cleaning.changes_to_apply_when_using_rulebook(rulebook, 'movisens_sensing')
        general_cleaning.execute_corrections_to_original_tables(ID_CLEAN_IMMERSE_PATH, 'movisens_sensing')  # TODO: Select a sub_folder: ['dmmh', 'maganamed', 'movisens_esm', 'movisens_sensing']


# Function to run ID validation from CSV/EXCEL files instead of SQL tables
def run_id_validation_from_df(reference_all_ids_directory, rulebook, test_directory, filename):
    if os.path.exists(reference_all_ids_directory):
        df_control = pd.read_excel(reference_all_ids_directory)

    esm_filepath = os.path.join(rulebook, "esm_rulebook.xlsx")
    if os.path.exists(esm_filepath):
        esm_rulebook_df = pd.read_excel(esm_filepath)
        # else: # TODO: Uncomment if this rulebook needs to be created. Note: ID changes must be added manually.
        #   get_ids_reference_esm()
        #   esm_rulebook_df = pd.read_excel(esm_filepath)

        for file in os.listdir(test_directory):
            if file.startswith(filename):
                if file.endswith(".csv"):
                    csv_df = pd.read_csv(os.path.join(test_directory, file))  # File(s) to validate
                    print(f"\n\033[34mFile to validate: '{file}' \033[0m\n")
                    general_validation_ids(df_control, esm_rulebook_df, csv_df, file)

                # elif file.endswith(".xlsx"):
                #     excel_df = pd.read_excel(os.path.join(test_directory, file))  # File(s) to validate
                #     print(f"\n\033[34mFile to validate:'{file}' \033[0m\n")
                #     general_validation_ids(df_control, esm_rulebook_df, excel_df, file)
    else:
        print(f"\n\033[34mFilepath not found!\033[0m\n")


def main():

    # -- Rule 0: ID validation.
    # Thi function runs a validation from  IDs only. From extracted IDs per SYSTEM, it compares and points out issues
    # according each "Master ID repository", files obtained by Anita as REFERENCE for IDS (using REDCap list,
    # MovisensESM files, and flowchart sheets which describes Participant IDs that were excluded since Baseline).
    # TODO: Include IDs to exclude from Flowchart-sheets from Anita.
    # TODO: Adjust function according each rulebook
    run_id_validation_from_df(IDS_REFERENCE_PATH, IDS_ESM_RULEBOOK_PATH, IDS_TO_VERIFY_PATH, 'extracted')

    # MAGANAMED:
    # Run all rules defined in IMMERSE DVP-V7.
    run_validation_maganamed()
    execute_corrections_maganamed(DB_PATH, ID_CLEAN_IMMERSE_PATH, IDS_MAGANAMED_RULEBOOK_PATH)


if __name__ == "__main__":
    main()
