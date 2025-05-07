import os
import pandas as pd
from main import connect_and_fetch_table
from config.config_loader import load_config_file


CHANGES_FILE_PATH = load_config_file('reports','changes')
FIXES_FILE_PATH = load_config_file('reports','fixes')


def cleaning_df(modifications_path, fixes_path):
    if modifications_path:

        original_kop_df = connect_and_fetch_table("Kind-of-participant")
        fixes_df = original_kop_df.copy()
        # print("original_df: \n", fixes_df.head(3))

        for file in os.listdir(modifications_path):
            print("Files: ", file)

            if 'kind'in file:
                modifications_df = pd.read_csv(os.path.join(modifications_path, file))
                # print("modifications_file: \n", modifications_df.info())

                for _, row in modifications_df.iterrows():
                    normalised_participants_id = row['participant_identifier'].upper()
                    fixes_df['participant_identifier'] = fixes_df['participant_identifier'].str.upper()
                    expected_value = row['Expected_value']

                    if normalised_participants_id in fixes_df['participant_identifier'].values:
                        if "-" in expected_value:
                            # print("Changes in ID: \n", expected_value)
                            fixes_df.loc[fixes_df['participant_identifier'] == normalised_participants_id, "participant_identifier"] = expected_value

                        elif len(str(expected_value)) == 1:
                            # print("Changes in Sites: \n", expected_value)
                            fixes_df.loc[fixes_df['participant_identifier'] == normalised_participants_id, "Site"] = int(expected_value)
                            fixes_df.loc[fixes_df['participant_identifier'] == normalised_participants_id, "SiteCode"] = int(expected_value)

                        else:
                            # print(f"Changes in center name \n {participants_id}: {expected_value}")
                            fixes_df.loc[fixes_df['participant_identifier'] == normalised_participants_id, "center_name"] = expected_value

                new_kop_filename = "FIXED-Kind-of-participant.csv"
                fixes_df.to_csv(os.path.join(fixes_path, new_kop_filename), index=False)
                print("Successfully cleaned file: \n", new_kop_filename)

# cleaning_df(CHANGES_FILE_PATH, FIXES_FILE_PATH)
