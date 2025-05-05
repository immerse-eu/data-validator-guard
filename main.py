import os
import sqlite3
import pandas as pd
import yaml
from validation.general_validation import DataValidator
from validation.maganamed_validation import VALID_SITE_CODES_AND_CENTER_NAMES, MaganamedValidation, import_custom_csr_df_with_language_selection

CSRI_list = ["CSRI", "CSRI_GE", "CSRI_BE", "CSRI_SK"]

def load_config_file(directory, file):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "config", "config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
        return config[directory][file]

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


def main():

    # -- MAGANAMED
    print("Runnning Maganamed Validation")

    # # -- Rule 1:
    read_kind_participants_df = connect_and_fetch_table("Kind-of-participant")
    general_magana_validation = DataValidator(read_kind_participants_df)
    rules_magana_validation = MaganamedValidation(read_kind_participants_df)


    valid_center_names = VALID_SITE_CODES_AND_CENTER_NAMES.values()
    first_control = general_magana_validation.check_typos(column="center_name", dictionary=valid_center_names)

    if first_control is not None:
        rules_magana_validation.validate_special_duplication_types(column="participant_identifier")
        rules_magana_validation.validate_site_and_center_name_id(
            site_column = "Site",
            center_name_column = "center_name",
            study_id_column="participant_identifier",
        )

    # # -- Rule 2:
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

if __name__ == "__main__":
    main()
