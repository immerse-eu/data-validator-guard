import os
import pandas as pd

from cleaning.general_id_cleaning import DataCleaning
from config.config_loader import load_config_file
from validation.general_validation import DataValidator
from validation.maganamed_validation import VALID_SITE_CODES_AND_CENTER_NAMES
from maganamed import run_validation_maganamed, execute_id_corrections_maganamed
from utils.rulebook import create_merged_esm_ids_rulebook
from cleaning.cleaning_db import cleaning_db
from movisensxs import run_movisensxs_validation

valid_center_names = VALID_SITE_CODES_AND_CENTER_NAMES.values()

DB_PATH = load_config_file('researchDB', 'db_path')
NEW_DB_PATH = load_config_file('researchDB', 'cleaned_db')

ISSUES_PATH = load_config_file('reports', 'issues')
CHANGES_PATH = load_config_file('reports', 'changes')
FIXES_PATH = load_config_file('reports', 'fixes')

IDS_REFERENCE_PATH = load_config_file('auxiliarFiles', 'ids_reference')  # RedCap IDs from Anita
IDS_TO_VERIFY_PATH = load_config_file('auxiliarFiles', 'ids_to_verify')  # Extracted IDs only from original files.

RULEBOOK_IDS_MAGANAMED_PATH = load_config_file('auxiliarFiles', 'ids_rulebook_maganamed')
RULEBOOK_IDS_MOVISENS_ESM_PATH = load_config_file('auxiliarFiles', 'ids_rulebook_esm')
RULEBOOK_IDS_MOVISENS_SENSING_PATH = load_config_file('auxiliarFiles', 'ids_reference_esm')  # TODO: Create file
RULEBOOK_IDS_REDCAP_PATH = load_config_file('auxiliarFiles', '')  #TODO: Update
RULEBOOK_IDS_DMMH_PATH = load_config_file('auxiliarFiles', 'ids_rulebook_dmmh')
ID_CLEANING_IMMERSE_PATH = load_config_file('updated_source',
                                            'immerse_clean')  # Here are stored a copy of sources which are changing


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
    if "maganamed" in file:
        print("Start cleaning maganamed...")
        execute_id_corrections_maganamed(ID_CLEANING_IMMERSE_PATH, RULEBOOK_IDS_MAGANAMED_PATH)

    elif "movisens_esm" in file:
        updated_rulebook = general_id_cleaning.prepare_ids_corrections(rulebook, CHANGES_PATH, file)
        general_id_cleaning.changes_to_apply_when_using_rulebook(updated_rulebook, 'movisens_esm')
        general_id_cleaning.execute_corrections_to_original_tables(ID_CLEANING_IMMERSE_PATH, 'movisens_esm')

    elif "movisens_sensing" in file:
        print("Cleaning movisens_sensing")
        # TODO: pending to do

    elif "dmmh" in file:
        print("Cleaning DMMH")
        general_id_cleaning.prepare_ids_corrections(rulebook, CHANGES_PATH, file)
        general_id_cleaning.changes_to_apply_when_using_rulebook(rulebook, 'dmmh')  # to verify
        general_id_cleaning.execute_corrections_to_original_tables(ID_CLEANING_IMMERSE_PATH, 'dmmh')

    elif "redcap" in file:
        print("Cleaning Redcap IDS...")
        general_id_cleaning.prepare_ids_corrections(rulebook, CHANGES_PATH, file)
        general_id_cleaning.changes_to_apply_when_using_rulebook(rulebook, 'redcap')  # to verify
        general_id_cleaning.execute_corrections_to_original_tables(ID_CLEANING_IMMERSE_PATH, 'redcap')

    else:
        print("IMMERSE system not recognised from 'dmmh', 'maganamed', 'movisens_esm', and 'movisens_sensing' ")


# Function to run ID validation from CSV/EXCEL files instead of SQL tables
def run_id_validation_from_df(reference_all_ids_directory, rulebook, test_directory, filename):
    if os.path.exists(reference_all_ids_directory):
        print("REDCap IDs path:", reference_all_ids_directory)
        df_control = pd.read_excel(reference_all_ids_directory)

    if os.path.exists(rulebook):
        print("Loading rulebook from: ", rulebook)
        rulebook_df = pd.read_csv(rulebook, sep=";") if rulebook.endswith('.csv') else pd.read_excel(rulebook, engine='openpyxl')

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
    """
    This function uses the extracted original IDS from each system and runs ID validation using the following files:
    - IDS_REFERENCE: REDCap Data Source from Maganamed. The most 'reliable' source available of IDS.
    - IDS_RULEBOOK: Defined rules to apply changes in IDS, such like: DELETE, UPDATE, ADD, MERGE, SKIP.
    - IDS_TO_VERIFY: Directory of files to verify against the rulebook and reference.
    """
    for filename in os.listdir(IDS_TO_VERIFY_PATH):
        if filename.startswith("extracted") and "maganamed" in filename:
            print("Maganamed", filename)
            run_id_validation_from_df(IDS_REFERENCE_PATH, RULEBOOK_IDS_MAGANAMED_PATH, IDS_TO_VERIFY_PATH, filename)

        elif filename.startswith("extracted") and "movisens_esm" in filename:
            print("Movisens_ESM", filename)
            run_id_validation_from_df(IDS_REFERENCE_PATH, RULEBOOK_IDS_MOVISENS_ESM_PATH, IDS_TO_VERIFY_PATH, filename)

        elif filename.startswith("extracted") and "movisens_sensing" in filename:
            print("Movisens_Sensing", filename)
            run_id_validation_from_df(IDS_REFERENCE_PATH, RULEBOOK_IDS_MOVISENS_SENSING_PATH, IDS_TO_VERIFY_PATH, filename)   # TODO

        elif filename.startswith("extracted") and "dmmh" in filename:  # TODO
            print("TODO: Create a Rulebook for DMMH IDS!")

        elif "redcap" in filename:
            run_id_validation_from_df(IDS_REFERENCE_PATH, RULEBOOK_IDS_REDCAP_PATH, IDS_TO_VERIFY_PATH, filename)



def main():

    # # --- Rule 0: ID validation.
    execute_immerse_id_validation()

    # # # --- MAGANAMED:
    # # Run all rules defined in IMMERSE DVP-V7.
    # run_validation_maganamed()
    # cleaning_db(NEW_DB_PATH, system='maganamed')
    #
    # # # --- MOVISENSXS
    # # Run all rules for Movisens-ESM & Movisens-Sensing defined in IMMERSE DVP-V7.
    # run_movisensxs_validation()


if __name__ == "__main__":
    main()
