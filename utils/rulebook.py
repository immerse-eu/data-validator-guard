import os
import pandas as pd
from config.config_loader import load_config_file


IDS_REFERENCE_PATH = load_config_file('auxiliarFiles', 'ids_reference')  # From Anita
ALL_IDS_ESM_REFERENCE_PATH = load_config_file('auxiliarFiles', 'all_ids_esm_reference')
RULEBOOK_IDS_MAGANAMED_PATH = load_config_file('auxiliarFiles', 'ids_rulebook_maganamed')
RULEBOOK_IDS_MOVISENS_ESM_PATH = load_config_file('auxiliarFiles', 'ids_rulebook_esm')


def create_merged_esm_ids_rulebook():
    reference_all_ids = []

    for file in os.listdir(ALL_IDS_ESM_REFERENCE_PATH):

        if not file.endswith("esm_ids_rulebook.xlsx") and '~' not in file:
            print(f"Processing file {file}")
            df = pd.read_excel(os.path.join(ALL_IDS_ESM_REFERENCE_PATH, file))
            df = df.rename(columns={'study_ID (MaganaMed)': 'participant_identifier'})
            df_filter = df[['participant_id', 'participant_movi_nr', 'VisitCode', 'SiteCode', 'participant_identifier']]
            df_filter = df_filter[df_filter['participant_id'] != "example"]
            reference_all_ids.append(df_filter)

    all_ids_ref_df = pd.concat(reference_all_ids)
    duplicates = all_ids_ref_df[all_ids_ref_df.duplicated()]
    print("Numer duplicates found:", len(duplicates))
    duplicates.to_excel(os.path.join(ALL_IDS_ESM_REFERENCE_PATH, "duplicated_ids_reference.xlsx"), index=False)

    filter_all_ids = all_ids_ref_df.drop_duplicates()
    filter_all_ids['action'] = filter_all_ids['participant_identifier'].apply(
        lambda x: "delete" if " " in str(x) or "delete" in str(x) or "TEST" in str(x) else (
            "update" if str(x).strip() and len(str(x).strip()) >= 10
            else "check manually"))
    filter_all_ids.to_excel(os.path.join(ALL_IDS_ESM_REFERENCE_PATH, "new_merged_esm_ids_rulebook.xlsx"), index=False)
    print(f"ESM rulebook created in: {ALL_IDS_ESM_REFERENCE_PATH}")


def get_columns_from_id_reference():
    id_reference_df = pd.read_excel(IDS_REFERENCE_PATH)
    id_reference_df = id_reference_df.rename(columns={'study_id_pat': 'correct_participant_identifier',
                                                      'ESMcondition': 'randomize'})
    interested_id_reference_df = id_reference_df[['correct_participant_identifier',
                                                  'site',
                                                  'condition',
                                                  'unit',
                                                  'randomize']]
    return interested_id_reference_df


def add_site_codes_to_rulebook():
    rulebook_df = pd.read_csv(RULEBOOK_IDS_MAGANAMED_PATH)
    interested_id_reference_df = get_columns_from_id_reference()

    merged_df = pd.merge(rulebook_df, interested_id_reference_df,
                           on='correct_participant_identifier', how='left')

    merged_df = merged_df[['participant_identifier',
                           'correct_participant_identifier',
                           'site',
                           'condition',
                           'unit',
                           'randomize',
                           'action'
                           ]]

    print(merged_df.info())

    path = os.path.dirname(RULEBOOK_IDS_MAGANAMED_PATH)
    file_name = "merged_rulebook_maganamed.csv"

    filepath = os.path.join(path, file_name)
    merged_df.to_csv(filepath, sep=';', index=False)
    print("Successfully saved")