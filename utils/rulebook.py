import os
import pandas as pd
from config.config_loader import load_config_file


IDS_REFERENCE_PATH = load_config_file('auxiliarFiles', 'ids_reference')  # From Anita
ALL_IDS_ESM_REFERENCE_PATH = load_config_file('auxiliarFiles', 'all_ids_esm_reference')
IDS_MAGANAMED_RULEBOOK_PATH = load_config_file('auxiliarFiles', 'ids_reference_maganamed')
IDS_ESM_RULEBOOK_PATH = load_config_file('auxiliarFiles', 'ids_reference_esm')
VALID_ESM_IDS_PATH = load_config_file('auxiliarFiles', 'ids_reference_esm')


def create_merged_esm_ids_rulebook():
    reference_all_ids = []

    for file in os.listdir(ALL_IDS_ESM_REFERENCE_PATH):
        print(f"Processing file {file}")

        if file.endswith("esm_ids_rulebook.xlsx") and not '~' in file:
            df = pd.read_excel(os.path.join(ALL_IDS_ESM_REFERENCE_PATH, file))
            df = df.rename(columns={'study_ID (MaganaMed)': 'participant_identifier'})
            df_filter = df[['participant_id', 'participant_movi_nr', 'SiteCode', 'participant_identifier']]
            df_filter = df_filter[df_filter['participant_id'] != "example"]
            reference_all_ids.append(df_filter)

    all_ids_ref_df = pd.concat(reference_all_ids)
    duplicates = all_ids_ref_df[all_ids_ref_df.duplicated()]
    print("Numer duplicates found:", len(duplicates))
    # duplicates.to_excel(os.path.join(VALID_ESM_IDS_PATH, "duplicated_ids_reference.xlsx"), index=False)
    filter_all_ids = all_ids_ref_df.drop_duplicates()
    filter_all_ids['action'] = filter_all_ids['participant_identifier'].apply(
        lambda x: "delete" if " " in str(x) or "delete" in str(x) or "TEST" in str(x) else (
            "update" if str(x).strip() and len(str(x).strip()) >= 10
            else "check manually"))
    filter_all_ids.to_excel(os.path.join(ALL_IDS_ESM_REFERENCE_PATH, "new_merged_esm_ids_rulebook.xlsx"), index=False)
    print(f"Excel file created in directory: {ALL_IDS_ESM_REFERENCE_PATH} ")
    return filter_all_ids


def get_columns_from_id_reference():
    id_reference_df = pd.read_excel(IDS_REFERENCE_PATH)
    # TODO: Verify with Anita
    id_reference_df = id_reference_df.rename(columns={'study_id_pat': 'correct_participant_identifier',
                                                      'ESMcondition': 'randomize'})
    interested_id_reference_df = id_reference_df[['correct_participant_identifier',
                                                  'site',
                                                  'condition',
                                                  'unit',
                                                  'randomize']]
    return interested_id_reference_df


def add_site_codes_to_rulebook():
    rulebook_df = pd.read_csv(IDS_MAGANAMED_RULEBOOK_PATH)
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

    path = os.path.dirname(IDS_MAGANAMED_RULEBOOK_PATH)
    file_name = "merged_rulebook_maganamed.csv"

    filepath = os.path.join(path, file_name)
    merged_df.to_csv(filepath, sep=';', index=False)
    print("Successfully saved")


def rulebook():
    # Create ESM rulebook. TODO: Uncomment when necessary. Manual changes are required to complete an ESM rulebook!
    # create_merged_esm_ids_rulebook()

    add_site_codes_to_rulebook()


rulebook()

