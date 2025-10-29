import pandas as pd
from validation.movisensxs_validation import MovisensxsValidation
from validation.general_validation import DataValidator
from config.config_loader import load_config_file
from utils.retrieve_participants_ids import read_all_dataframes

ISSUES_PATH = load_config_file('reports', 'issues')
CHANGES_PATH = load_config_file('reports', 'changes')
FIXES_PATH = load_config_file('reports', 'fixes')
NEW_DB_PATH = load_config_file('researchDB', 'cleaned_db')

IDS_REFERENCE_PATH = load_config_file('auxiliarFiles', 'ids_reference')  # From Anita
IMMERSE_CLEANING_SOURCE = load_config_file('updated_source', 'immerse_clean')

fidelity_files = [
    "Fidelity_BE.xlsx",
    "Fidelity_c_UK.xlsx",
    "Fidelity_GE.xlsx",
    "Fidelity_SK.xlsx",
    "Fidelity_UK.xlsx",
    "IMMERSE_Fidelity_SK_Kosice.xlsx"
]


# Rule 14 from DVP: Filename contains right data that fits with "Visit" and "Country" selection.
def movisensxs_rule_fourteen(df, table_name):
    print(f"\n\033[95m Validating '{table_name}' for Visit and Country selection:\033[0m\n")
    rules_movisensxs_validation = MovisensxsValidation(df)
    rules_movisensxs_validation.validate_visit_and_site_assignation(table_name)
    result_validation = rules_movisensxs_validation.passed_validation(table_name, ISSUES_PATH)
    return result_validation


# Rule 16 from DVP: Match participant IDs with Maganamed IDs.
# Rule 17 from DVP: Match clinicians IDs with Maganamed IDs.
def movisensxs_rule_sixteen_and_seventeen(df, table_name):
    print(f"\n\033[95m Rule 16 and 17 from DVP. Validating '{table_name}' with Maganamed IDS\033[0m\n")
    maganamed_ids_reference_df = pd.read_excel(IDS_REFERENCE_PATH)

    validate_ids_from_fidelity_files_with_maganamed = DataValidator(df)
    validate_ids_from_fidelity_files_with_maganamed.compare_ids_with_redcap_ids(maganamed_ids_reference_df, 0)


def run_movisensxs_validation():
    # --- Movisens_ESM ---
    dfs, filenames = read_all_dataframes(IMMERSE_CLEANING_SOURCE, 'movisens_esm')
    for df, filename in zip(dfs, filenames):
        is_validation_approved = movisensxs_rule_fourteen(df, filename)
        if not is_validation_approved:
            print("Changes mus be applied in", filename, "\n")

    # --- Movisens_Fidelity ---
    dfs, filenames = read_all_dataframes(IMMERSE_CLEANING_SOURCE, 'movisens_esm')
    print("Fidelity files")
    for df, filename in zip(dfs, filenames):
        movisensxs_rule_sixteen_and_seventeen(df, filename)
