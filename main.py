import os
import pandas as pd

from cleaning.general_id_cleaning import DataCleaning
from config.config_loader import load_config_file
from validation.general_validation import DataValidator
from validation.maganamed_validation import VALID_SITE_CODES_AND_CENTER_NAMES
from maganamed import run_validation_maganamed, execute_id_corrections_maganamed
from utils.rulebook import create_merged_esm_ids_rulebook

valid_center_names = VALID_SITE_CODES_AND_CENTER_NAMES.values()

DB_PATH = load_config_file('researchDB', 'db_path')
NEW_DB_PATH = load_config_file('researchDB', 'cleaned_db')

ISSUES_PATH = load_config_file('reports', 'issues')
CHANGES_PATH = load_config_file('reports', 'changes')
FIXES_PATH = load_config_file('reports', 'fixes')

IDS_REFERENCE_PATH = load_config_file('auxiliarFiles', 'ids_reference')  # From Anita
IDS_TO_VERIFY_PATH = load_config_file('auxiliarFiles', 'ids_to_verify')  # Extracted IDs only from GH.
IDS_MAGANAMED_RULEBOOK_PATH = load_config_file('auxiliarFiles', 'ids_reference_maganamed')
IDS_ESM_RULEBOOK_PATH = load_config_file('auxiliarFiles', 'ids_reference_esm')
ID_CLEANING_IMMERSE_PATH = load_config_file('updated_source', 'immerse_clean')


# General initial rule: ID validation
def general_validation_ids(df_control, rulebook, df_to_validate, file):
    print(f"\n\033[95m Validating IDS :\033[0m\n")
    general_validation = DataValidator(df_to_validate)
    general_validation.check_general_duplications(df_to_validate)
    general_validation.check_duplications_applying_normalisation('participant_identifier')
    general_validation.compare_ids_with_redcap_ids(df_control, id_column=0)
    general_validation.check_typos_in_ids(id_column=0)
    df_report = general_validation.report(os.path.join(ISSUES_PATH, "ID_issues"), file)

    #  Cleaning process
    general_id_cleaning = DataCleaning(df_report)
    if "movisens_esm" in file:
        general_id_cleaning.prepare_ids_correction_from_esm(rulebook, CHANGES_PATH, file)
        general_id_cleaning.changes_to_apply_when_using_rulebook(rulebook, 'movisens_sensing')  # to verify
        general_id_cleaning.execute_corrections_to_original_tables(ID_CLEANING_IMMERSE_PATH, 'movisens_sensing')

    elif "maganamed" in file:
        print("Cleaning Maganamed")
        execute_id_corrections_maganamed(ID_CLEANING_IMMERSE_PATH, IDS_MAGANAMED_RULEBOOK_PATH)

    elif "movisens_sensing" in file:
        print("Cleaning movisens_sensing")
        # TODO: pending to do

    elif "dmmh" in file:
        print("Cleaning dmmh")
        # TODO: pending to do

    else:
        print("IMMERSE system not recognised from 'dmmh', 'maganamed', 'movisens_esm', and 'movisens_sensing' ")


# Function to run ID validation from CSV/EXCEL files instead of SQL tables
def run_id_validation_from_df(reference_all_ids_directory, rulebook, test_directory, filename):
    if os.path.exists(reference_all_ids_directory):
        df_control = pd.read_excel(reference_all_ids_directory)

    if os.path.exists(rulebook):
        print("Loading rulebook from: ", rulebook)
        rulebook_df = pd.read_csv(rulebook)

        for file in os.listdir(test_directory):
            if file.startswith(filename):
                if file.endswith(".csv"):
                    csv_df = pd.read_csv(os.path.join(test_directory, file))  # File(s) to validate
                    print(f"\n\033[34mFile to validate: '{file}' \033[0m\n")
                    general_validation_ids(df_control, rulebook_df, csv_df, file)

                # elif file.endswith(".xlsx"):
                #     excel_df = pd.read_excel(os.path.join(test_directory, file))  # File(s) to validate
                #     print(f"\n\033[34mFile to validate:'{file}' \033[0m\n")
                #     general_validation_ids(df_control, rulebook_df, excel_df, file)
    else:
        print(f"\n\033[34mFilepath for rulebook not found!\033[0m\n")
        create_merged_esm_ids_rulebook()  # TODO: After the file is created, changes must be added manually!


def execute_immerse_id_validation():
    for filename in os.listdir(IDS_TO_VERIFY_PATH):

        if filename.startswith("extracted") and "maganamed" in filename:
            print("Magana", filename)
            run_id_validation_from_df(IDS_REFERENCE_PATH, IDS_MAGANAMED_RULEBOOK_PATH, IDS_TO_VERIFY_PATH, filename)

        elif filename.startswith("extracted") and "movisens" in filename:
            print("Movisens", filename)
            # run_id_validation_from_df(IDS_REFERENCE_PATH, IDS_ESM_RULEBOOK_PATH, IDS_TO_VERIFY_PATH, filename)

        elif filename.startswith("extracted") and "dmmh" in filename:
            print("TODO: Create a Rulebook for DMMH IDS!")


def main():

    # -- Rule 0: ID validation.
    # This function runs a validation from  IDs only. From extracted IDs per SYSTEM, it compares and points out issues
    # according each "Master ID repository", files obtained by Anita as REFERENCE for IDS (using REDCap list,
    # MovisensESM files, and flowchart sheets which describes Participant IDs that were excluded since Baseline).
    # TODO: Include IDs to exclude from Flowchart-sheets from Anita.
    # TODO: Adjust function according each rulebook
    execute_immerse_id_validation()

    # # MAGANAMED:
    # # Run all rules defined in IMMERSE DVP-V7.
    # run_validation_maganamed()
    # execute_corrections_maganamed(DB_PATH, ID_CLEAN_IMMERSE_PATH, IDS_MAGANAMED_RULEBOOK_PATH)


if __name__ == "__main__":
    main()
