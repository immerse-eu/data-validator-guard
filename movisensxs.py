import os
import pandas as pd
from validation.movisensxs_validation import MovisensxsValidation
from config.config_loader import load_config_file
from database.db import connect_and_fetch_table

ISSUES_PATH = load_config_file('reports', 'issues')
CHANGES_PATH = load_config_file('reports', 'changes')
FIXES_PATH = load_config_file('reports', 'fixes')
NEW_DB_PATH = load_config_file('researchDB', 'cleaned_db')

movisens_esm_filenames = [
    'IMMERSE_T0_BE',
    'IMMERSE_T0_GE',
    'IMMERSE_T0_SK',
    'IMMERSE_T0_SK_Female',
    'IMMERSE_T0_SK_Kosice',
    'IMMERSE_T0_SK_Kosice_Female',
    'IMMERSE_T0_UK',
    'IMMERSE_T1_BE',
    'IMMERSE_T1_GE',
    'IMMERSE_T1_SK',
    'IMMERSE_T1_SK_Female',
    'IMMERSE_T1_SK_Kosice',
    'IMMERSE_T1_SK_Kosice_Female',
    'IMMERSE_T1_UK',
    'IMMERSE_T2_BE',
    'IMMERSE_T2_GE',
    'IMMERSE_T2_SK',
    'IMMERSE_T2_SK_Female',
    'IMMERSE_T2_SK_Kosice',
    'IMMERSE_T2_SK_Kosice_Female',
    'IMMERSE_T2_UK',
    'IMMERSE_T3_BE',
    'IMMERSE_T3_GE',
    'IMMERSE_T3_SK',
    'IMMERSE_T3_SK_Female',
    'IMMERSE_T3_SK_Kosice',
    'IMMERSE_T3_SK_Kosice_Female',
    'IMMERSE_T3_UK',
]


# Rule X: Filename contains right data that fits with "Visit" and "Country" selection.
def movisensxs_rule_one(df, filename):
    rules_movisensxs_validation = MovisensxsValidation(df)
    rules_movisensxs_validation.validate_visit_and_country_assignation(filename)


# TODO: Upload movisensxs into DB
def run_movisensxs_validation():
    for filename in movisens_esm_filenames:
        read_df = connect_and_fetch_table(filename)
        print(read_df.info())
        movisensxs_rule_one(read_df, filename)


run_movisensxs_validation()
