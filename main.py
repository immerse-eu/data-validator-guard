import sqlite3
import pandas as pd
import yaml
from validation.general_validation import DataValidator
from validation.maganamed_validation import VALID_SITE_CODES_AND_CENTER_NAMES, MaganamedValidation


with open("./config/config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

DB_PATH = config['researchDB']['db_path']


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

    read_df = connect_and_fetch_table("Kind-of-participant")
    general_magana_validation = DataValidator(read_df)
    rules_magana_validation = MaganamedValidation(read_df)


    valid_center_names = VALID_SITE_CODES_AND_CENTER_NAMES.values()
    first_control = general_magana_validation.check_typos(column="center_name", dictionary=valid_center_names)

    if first_control is not None:
        rules_magana_validation.special_duplication_types(column="participant_identifier")
        rules_magana_validation.validate_site_and_center_name_id(
            site_column = "Site",
            center_name_column = "center_name",
            study_id_column="participant_identifier",
        )

    # # -- EXTRA ACTION: SEARCH
    # input_value = ['ABC', 'CBA']        # TODO: Change these values for real IDs or value to search.
    # execute_search(input_value)

if __name__ == "__main__":
    main()
