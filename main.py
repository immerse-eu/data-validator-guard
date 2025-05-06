import sqlite3
import pandas as pd
from config.config_loader import load_config_file
from validation.general_validation import DataValidator
from validation.maganamed_validation import (
    VALID_SITE_CODES_AND_CENTER_NAMES, MaganamedValidation, import_custom_csr_df_with_language_selection)
from cleaning import cleaning_df


CSRI_list = ["CSRI", "CSRI_GE", "CSRI_BE", "CSRI_SK"]
valid_center_names = VALID_SITE_CODES_AND_CENTER_NAMES.values()

DB_PATH = load_config_file('researchDB','db_path')
FIXES_PATH = load_config_file('reports','fixes')

def connect_and_fetch_table(table_name):
    sql_connection = sqlite3.connect(DB_PATH)
    try:
        query = f"SELECT * FROM `{table_name}`"
        df = pd.read_sql_query(query, sql_connection)
    finally:
        sql_connection.close()
    return df

def run_general_validation(filename):
    general_magana_validation = DataValidator(filename)
    general_magana_validation.check_typos(column="center_name", dictionary=valid_center_names)
    general_magana_validation.check_duplicates(filename)
    general_magana_validation.report()
    is_approved = general_magana_validation.passed_validation()
    return is_approved

def run_rule_one(filename):
    rules_magana_validation = MaganamedValidation(filename)
    rules_magana_validation.validate_special_duplication_types(column="participant_identifier")
    rules_magana_validation.validate_site_and_center_name_id(
        site_column="Site",
        center_name_column="center_name",
        study_id_column="participant_identifier",
    )

def main():

    print("\n Running General Validation:")

    # -- Rule 1: Apply validation for 'Kind-of-participant'.
    read_kind_participants_df = connect_and_fetch_table("Kind-of-participant")
    is_validation_approved = run_general_validation(read_kind_participants_df)
    if is_validation_approved:
        print("\n Running Maganamed Validation:")
        run_rule_one(read_kind_participants_df)
    else:
        print("FIX general errors before proceeding with validation")

    #  -- Rule 2:
    # Preprocessing:
    crsi_df = import_custom_csr_df_with_language_selection()
    managa_rules_for_crsi_validation = MaganamedValidation(crsi_df)
    managa_rules_for_crsi_validation.validate_site_and_center_name_id(
        site_column="Site",
        center_name_column = "center_name",
        study_id_column="participant_identifier",
    )

    for csri_table in CSRI_list:
        read_csri_df = connect_and_fetch_table(csri_table)
        general_magana_validation = DataValidator(read_csri_df)
        rules_magana_validation = MaganamedValidation(read_csri_df)

        print(f"\n\033[34mTable {csri_table} overview :\033[0m\n")
        general_magana_validation.check_typos(column="center_name", dictionary=valid_center_names)
        rules_magana_validation.validate_site_and_center_name_id(
            site_column = "SiteCode",
            center_name_column = "center_name",
            study_id_column="participant_identifier"
        )

    # # -- EXTRA ACTION: SEARCH
    # input_value = ['ABC', 'CBA']        # TODO: Change these values for real IDs or value to search.
    # execute_search(input_value)

    # # Control fixed, generated file from Rule 1.
    # for filename in os.listdir(FIXES_PATH):
    #     if "Kind-of-participant" in filename and filename.endswith(".csv"):
    #         read_new_kind_participants_df = pd.read_csv(os.path.join(FIXES_PATH, filename))
    #         run_rule_one(read_new_kind_participants_df)

    # # ORIGINAL FILE
    # filename = "original-Kind-of-participant.csv"
    # if "Kind-of-participant" in filename and filename.endswith(".csv"):
    #     read_new_kind_participants_df = pd.read_csv(filename, sep=';')
    #     print(read_new_kind_participants_df.head(3))
    #     run_general_validation(read_new_kind_participants_df)

if __name__ == "__main__":
    main()
