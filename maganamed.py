import pandas as pd

from config.config_loader import load_config_file
from database.db import connect_and_fetch_table
from validation.general_validation import DataValidator
from validation.maganamed_validation import (VALID_SITE_CODES_AND_CENTER_NAMES, MaganamedValidation,
                                             import_custom_csr_df_with_language_selection)
from utils.create_auxiliar_files import filter_only_participants
from cleaning.general_id_cleaning import DataCleaning

CSRI_list = ["CSRI", "CSRI_GE", "CSRI_BE", "CSRI_SK"]
valid_center_names = VALID_SITE_CODES_AND_CENTER_NAMES.values()

DB_PATH = load_config_file('researchDB', 'db_path')
NEW_DB_PATH = load_config_file('researchDB', 'cleaned_db')


def run_general_validation(table):
    print(f"\n\033[95m Validating General rules:\033[0m\n")
    general_magana_validation = DataValidator(table)
    general_magana_validation.check_typos(column="center_name", dictionary=valid_center_names)
    general_magana_validation.check_general_duplications(table)
    # general_magana_validation.report()
    is_approved = general_magana_validation.passed_validation()
    return is_approved


# Rule 1: SITE and CENTER_NAME must fit according DVM-V7
def run_rule_one(table, table_name):
    print(f"\n\033[95m Validating for SITE and CENTER_NAME:\033[0m\n")
    rules_magana_validation = MaganamedValidation(table)
    rules_magana_validation.validate_special_duplication_types(column="participant_identifier")
    rules_magana_validation.validate_site_and_center_name_id(
        site_column="SiteCode",  # Change to 'Site', depending on the table
        center_name_column="center_name",
        study_id_column="participant_identifier",
    )
    is_validation_approved = rules_magana_validation.passed_validation(table_name)
    return is_validation_approved


# Auxiliar function for Rule 8.
def run_auxiliary_rule_eight(table):
    print(f"\n\033[95mValidating auxiliar file for Language selection:\033[0m\n")

    rules_magana_validation = MaganamedValidation(table)
    participant_language_result = rules_magana_validation.validate_auxiliar_table(
        study_id_column="participant_identifier",
        center_name_column="center_name",
    )
    return participant_language_result


# Rule 8:  For CSRI questionnaires, LANGUAGE selection must fit according to FILENAME according DVM-V7.
def run_rule_eight(table, filename):
    print(f"\n\033[95m Validating Language selection per tables:\033[0m\n")

    rules_magana_validation = MaganamedValidation(table)
    rules_magana_validation.validate_language_selection(
        table_name=filename,
        site_column="SiteCode",
    )


# Combined rule 9 and 12 since the validation applies for the same file: SAQ
# Rule 9. Completion of questionnaires at least 80 %.
# Rule 12. Assesses real Visit TIME since user start - finished test. Compare with Baseline, T1, T2, and T3 values
def run_rule_nine_and_twelve(table, table_name):
    rules_magana_validation = MaganamedValidation(table)
    print(f"\n\033[95m Validating {table_name} completion min 80% :\033[0m\n")
    updated_table = rules_magana_validation.validate_completion_questionnaires(table_name)
    print(f"\n\033[95m Validating '{table_name}' correct Time selection (T1-T3):\033[0m\n")
    rules_magana_validation.validate_periods(table_name)


# Rule 11. Correct diagnosis selection.
def run_rule_eleven(table, table_name):
    print(f"\n\033[95m Validating from '{table_name}' correct diagnosis selection:\033[0m\n")
    rules_magana_validation = MaganamedValidation(table)
    rules_magana_validation.validate_primary_diagnosis(table_name)


# Rule 13. End comparison
def run_rule_thirteen(table, table_name):
    rules_magana_validation = MaganamedValidation(table)
    rules_magana_validation.validate_completed_visits(table_name)


# Auxiliar rule No. 13
def run_auxiliary_rule_thirteen(table):
    rules_magana_validation = MaganamedValidation(table)
    return rules_magana_validation.retrieve_saq_data()


def execute_id_corrections_maganamed(clean_source_path, rulebook):
    rulebook_df = pd.read_csv(rulebook)
    maganamed_cleaning = DataCleaning(rulebook_df)
    maganamed_cleaning.changes_to_apply_when_using_rulebook(rulebook_df, 'maganamed')  # DONE :D
    maganamed_cleaning.execute_corrections_to_original_tables(clean_source_path, "maganamed")


def run_validation_maganamed():
    # -- Rule 1: Apply validation for 'Kind-of-participant'.
    table_name = "Kind-of-participant"
    read_kind_participants_df = connect_and_fetch_table(table_name)
    filter_read_kind_participants_df = filter_only_participants(read_kind_participants_df, "participant_identifier")
    is_validation_approved = run_general_validation(filter_read_kind_participants_df)
    if is_validation_approved:
        run_rule_one(filter_read_kind_participants_df, table_name)

    # -- Rule 8: CSRI Language control and questionnaires completion
    # Part 1.
    auxiliar_csri_df = import_custom_csr_df_with_language_selection()
    run_general_validation(auxiliar_csri_df)
    participant_language_result = run_auxiliary_rule_eight(auxiliar_csri_df)

    # Part 2.
    for csri_table in CSRI_list:

        print(f"\n\033[34mTable {csri_table} overview :\033[0m\n")
        read_csri_df = connect_and_fetch_table(csri_table)
        run_general_validation(read_csri_df)

        if "_" in csri_table:
            table_abbrev = csri_table.split('_')[1]
            run_rule_eight(read_csri_df, table_abbrev)
        else:
            sample = list(read_csri_df['participant_identifier'])
            control = list(participant_language_result['participant_identifier'])
            if all(item in control for item in sample):
                print(f"\n ✔ | Language validation from '{csri_table}', successfully passed")
            else:
                print(f"Participant from {csri_table} has no invalid language")

    # -- Run Rule 9 and 12
    table_name = 'Service-Attachement-Questionnaire-(SAQ)'
    read_saq_df = connect_and_fetch_table(table_name)
    run_rule_nine_and_twelve(read_saq_df, table_name)

    # -- Run Rule 11: Correct diagnosis selection
    table_name = 'Diagnosis'
    read_diagnosis_df = connect_and_fetch_table(table_name)
    run_rule_eleven(read_diagnosis_df, table_name)

    # -- Run Rule 13: Completed visits
    read_end_df = connect_and_fetch_table('End')
    filter_end_df = filter_only_participants(read_end_df, 'participant_identifier')
    read_saq_df = connect_and_fetch_table('Service-Attachement-Questionnaire-(SAQ)')
    new_saq_df = run_auxiliary_rule_thirteen(read_saq_df)
    run_rule_thirteen(filter_end_df, new_saq_df)
