import os
import sqlite3
import pandas as pd

from cleaning.general_id_cleaning import DataCleaning, create_merged_esm_ids_rulebook
from config.config_loader import load_config_file
from validation.general_validation import DataValidator
from validation.maganamed_validation import (
    VALID_SITE_CODES_AND_CENTER_NAMES, MaganamedValidation, import_custom_csr_df_with_language_selection)
from cleaning import cleaning_df
from validation.seach_values import execute_search

CSRI_list = ["CSRI", "CSRI_GE", "CSRI_BE", "CSRI_SK"]
valid_center_names = VALID_SITE_CODES_AND_CENTER_NAMES.values()

DB_PATH = load_config_file('researchDB', 'db_path')
NEW_DB_PATH = load_config_file('researchDB', 'cleaned_db')

ISSUES_PATH = load_config_file('reports', 'issues')
CHANGES_PATH = load_config_file('reports', 'changes')
FIXES_PATH = load_config_file('reports', 'fixes')

ID_CONTROL_PATH = load_config_file('auxiliarFiles', 'ids_reference')
ID_SAMPLE_PATH = load_config_file('auxiliarFiles', 'ids_to_verify')
ID_ESM_RULEBOOK_PATH = load_config_file('auxiliarFiles', 'ids_reference_esm')
NEW_SOURCE_PATH = load_config_file('updated_source', 'immerse_clean')


def connect_and_fetch_table(table_name):
    sql_connection = sqlite3.connect(NEW_DB_PATH)
    try:
        query = f"SELECT * FROM `{table_name}`"
        df = pd.read_sql_query(query, sql_connection)
    finally:
        sql_connection.close()
    return df


def run_general_validation(table):
    print(f"\n\033[95m Validating General rules:\033[0m\n")
    general_magana_validation = DataValidator(table)
    general_magana_validation.check_typos(column="center_name", dictionary=valid_center_names)
    general_magana_validation.check_general_duplications(table)
    # general_magana_validation.report()
    is_approved = general_magana_validation.passed_validation()
    return is_approved


# Rule 1: SITE and CENTER_NAME must fit according DVM-V7
def run_rule_one(table):
    print(f"\n\033[95m Validating for SITE and CENTER_NAME:\033[0m\n")
    rules_magana_validation = MaganamedValidation(table)
    rules_magana_validation.validate_special_duplication_types(column="participant_identifier")
    rules_magana_validation.validate_site_and_center_name_id(
        site_column="SiteCode",  # Change to 'Site', depending on the table
        center_name_column="center_name",
        study_id_column="participant_identifier",
    )


def run_auxiliary_rule_two(table):
    print(f"\n\033[95mValidating extended Language selection:\033[0m\n")
    # Part 1.
    rules_magana_validation = MaganamedValidation(table)
    participant_language_result = rules_magana_validation.validate_auxiliar_table(
        study_id_column="participant_identifier",
        center_name_column="center_name",
    )
    return participant_language_result


# Rule 2: LANGUAGE selection must fit according FILENAME according DVM-V7
def run_rule_two(table, filename):
    print(f"\n\033[95m Validating Language selection per tables:\033[0m\n")

    # Part 2.
    rules_magana_validation = MaganamedValidation(table)
    rules_magana_validation.validate_language_selection(
        table_name=filename,
        site_column="SiteCode",
    )


# Rule 3. Completion of questionnaires at least 80 %.
def run_rule_three(table, table_name):
    print(f"\n\033[95m Validating {table_name} completion:\033[0m\n")
    rules_magana_validation = MaganamedValidation(table)
    rules_magana_validation.validate_completion_questionaries(table_name)


# Rule 4. Correct diagnosis selection.
def run_rule_four(table, table_name):
    print(f"\n\033[95m Validating from '{table_name}' correct diagnosis selection:\033[0m\n")
    rules_magana_validation = MaganamedValidation(table)
    rules_magana_validation.validate_primary_diagnosis(table_name)


# Rule 5. Calculate real TIME since user start - finished test. Compare with T1, T2, T3
def run_rule_five(table, table_name):
    print(f"\n\033[95m Validating '{table_name}' correct Time selection (T1-T3):\033[0m\n")
    rules_magana_validation = MaganamedValidation(table)
    rules_magana_validation.validate_periods(table_name)


# Rule 6. End comparison
def run_rule_six(table, table_name):
    rules_magana_validation = MaganamedValidation(table)
    rules_magana_validation.validate_completed_visits(table_name)


def run_auxiliary_rule_six(table):
    rules_magana_validation = MaganamedValidation(table)
    return rules_magana_validation.retrieve_saq_data()


# General initial rule: ID validation
def general_validation_ids(df_control, esm_rulebook, df_to_validate, file):
    print(f"\n\033[95m Validating IDS :\033[0m\n")
    general_validation = DataValidator(df_to_validate)
    general_validation.check_general_duplications(df_to_validate)
    general_validation.check_duplications_applying_normalisation('participant_identifier')
    general_validation.compare_ids_with_redcap_ids(df_control, id_column=0)
    general_validation.check_typos_in_ids(id_column=0)
    df_report = general_validation.report(ISSUES_PATH, file)
    # Cleaning process
    general_cleaning = DataCleaning(df_report)
    general_cleaning.ids_structure_correction(esm_rulebook, CHANGES_PATH, NEW_SOURCE_PATH, file)


# Alternative function to run ID validation from CSV/EXCEL files instead of SQL tables
def run_id_validation_from_df(reference_all_ids_directory, esm_rulebook, test_directory, filename):
    if os.path.exists(reference_all_ids_directory):
        df_control = pd.read_excel(reference_all_ids_directory)

    esm_filepath = os.path.join(esm_rulebook, "merged_esm_ids_rulebook.xlsx")
    if os.path.exists(esm_filepath):
        esm_rulebook_df = pd.read_excel(esm_filepath)
    # else: # TODO: Uncomment if this rulebook needs to be created.
        # get_ids_reference_esm()
        # esm_rulebook_df = pd.read_excel(esm_filepath)

        for file in os.listdir(test_directory):
            if file.startswith(filename):
                if file.endswith(".csv"):
                    csv_df = pd.read_csv(os.path.join(test_directory, file))  # File(s) to validate
                    print(f"\n\033[34mFile to validate: '{file}' \033[0m\n")
                    general_validation_ids(df_control, esm_rulebook_df, csv_df, file)

                elif file.endswith(".xlsx"):
                    excel_df = pd.read_excel(os.path.join(test_directory, file))  # File(s) to validate
                    print(f"\n\033[34mFile to validate:'{file}' \033[0m\n")
                    general_validation_ids(df_control, esm_rulebook_df, excel_df, file)
    else:
        print(f"\n\033[34mFilepath not found!\033[0m\n")


def main():

    # -- Rule 0: ID validation
    run_id_validation_from_df(ID_CONTROL_PATH, ID_ESM_RULEBOOK_PATH, ID_SAMPLE_PATH, 'extracted')

    # -- Rule 1: Apply validation for 'Kind-of-participant'.
    read_kind_participants_df = connect_and_fetch_table("Kind-of-participant")
    is_validation_approved = run_general_validation(read_kind_participants_df)
    if is_validation_approved:
        run_rule_one(read_kind_participants_df)

    # -- Rule 2: CSRI Language control and questionaries completion
    # Part 1.
    auxiliar_csri_df = import_custom_csr_df_with_language_selection()
    run_general_validation(auxiliar_csri_df)
    participant_language_result = run_auxiliary_rule_two(auxiliar_csri_df)

    # Part 2.
    for csri_table in CSRI_list:

        print(f"\n\033[34mTable {csri_table} overview :\033[0m\n")
        read_csri_df = connect_and_fetch_table(csri_table)
        run_general_validation(read_csri_df)

        if "_" in csri_table:
            table_abbrev = csri_table.split('_')[1]
            run_rule_two(read_csri_df, table_abbrev)
        else:
            sample = list(read_csri_df['participant_identifier'])
            control = list(participant_language_result['participant_identifier'])
            if all(item in control for item in sample):
                print(f"\n ✔ | Language validation from '{csri_table}', successfully passed")
            else:
                print(f"Participant from {csri_table} has no invalid language")

    # -- Run Rule 3: Questionaries completion
    table_name = 'Service-Attachement-Questionnaire-(SAQ)'
    read_saq_df = connect_and_fetch_table(table_name)
    run_rule_three(read_saq_df, table_name)

    # -- Run Rule 4: Correct diagnosis selection
    table_name = 'Diagnosis'
    read_diagnosis_df = connect_and_fetch_table(table_name)
    run_rule_four(read_diagnosis_df, table_name)

    # -- Run Rule 5: "Visit time points"
    table_name = 'Service-Attachement-Questionnaire-(SAQ)'
    read_saq_df = connect_and_fetch_table(table_name)
    run_rule_five(read_saq_df, table_name)

    # -- Run Rule 6: Completed visits
    read_end_df = connect_and_fetch_table('End')
    read_saq_df = connect_and_fetch_table('Service-Attachement-Questionnaire-(SAQ)')
    new_saq_df = run_auxiliary_rule_six(read_saq_df)
    run_rule_six(read_end_df, new_saq_df)

    # -- EXTRA ACTION: SEARCH
    input_value = ['Screening']        # TODO: Change these values for real IDs or value to search.
    execute_search(input_value)

    # Control fixed, generated file from Rule 1.
    for filename in os.listdir(FIXES_PATH):
        if "Kind-of-participant" in filename and filename.endswith(".csv"):
            read_new_kind_participants_df = pd.read_csv(os.path.join(FIXES_PATH, filename))
            run_rule_one(read_new_kind_participants_df)

    # ORIGINAL FILE
    filename = "original-Kind-of-participant.csv"
    if "Kind-of-participant" in filename and filename.endswith(".csv"):
        read_new_kind_participants_df = pd.read_csv(filename, sep=';')
        print(read_new_kind_participants_df.head(3))
        run_general_validation(read_new_kind_participants_df)


if __name__ == "__main__":
    main()
