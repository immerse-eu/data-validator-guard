import os
import re
import pandas as pd
from pandas import read_excel

from config.config_loader import load_config_file

VALID_CENTER_ACRONYMS = ["BI", "LE", "MA", "WI", "BR", "KO", "CA", "LO"]
VALID_PARTICIPANT_TYPES = ["P", "C", "A"]

VALID_PATTERN_USING_MINUS = re.compile(r"^I-(" + "|".join(VALID_CENTER_ACRONYMS) + r")-(" + "|".join(VALID_PARTICIPANT_TYPES) + r")-\d{3}$")
VALID_PATTERN_USING_UNDERSCORE = re.compile(r"^I_(" + "|".join(["WI", "MA"]) + r")_(" + "|".join(VALID_PARTICIPANT_TYPES) + r")_\d{3}$")

VALID_ESM_IDS_PATH = load_config_file('auxiliarFiles', 'ids_reference_esm')


# TODO: Uncomment method when "merged_ids_reference.xlsx" needs to be created.
# def get_ids_reference_esm():
#     reference_all_ids = []
#
#     for file in os.listdir(VALID_ESM_IDS_PATH):
#         # print(f"Processing file {file}")
#
#         if file.endswith(".xlsx") and not '~' in file:
#             df = pd.read_excel(os.path.join(VALID_ESM_IDS_PATH, file))
#             df_filter = df[['participant_id', 'study_ID (MaganaMed)']]
#             reference_all_ids.append(df_filter)
#
#     all_ids_ref_df = pd.concat(reference_all_ids)
#     filter_all_ids = all_ids_ref_df.drop_duplicates()
#     filter_all_ids.to_excel(os.path.join(VALID_ESM_IDS_PATH, "merged_ids_reference.xlsx"), index=False)
#     return filter_all_ids


class DataCleaning:

    def __init__(self, df):
        self.df = df
        self.clean_df = df.copy()

    # This outcome is generated from Anita files allESM
    def ids_correction_for_movisensxs(self, fixes_path, filename):
        df_issues = self.df.copy()
        # generate_esm_ids_reference = get_ids_reference_esm()  # TODO: uncomment to create 'merged_ids_reference.xlsx'
        merged_ids_reference_df = read_excel(VALID_ESM_IDS_PATH)
        merged_ids_reference_df.rename(columns={merged_ids_reference_df.columns[0]: 'participant_identifier'},
                                       inplace=True)
        merged_ids_reference_df.rename(columns={merged_ids_reference_df.columns[1]: 'correct_participant_identifier'},
                                       inplace=True)

        df_fixes = pd.merge(df_issues, merged_ids_reference_df, on='participant_identifier', how='left')
        df_fixes_filtered = df_fixes.drop_duplicates()
        # df_fixes_filtered.to_csv(os.path.join(fixes_path, f'fixes_{filename}'), index=False)
        self.df = df_fixes_filtered
        return self

    def ids_correction_by_regex(self, fixes_path, filename):
        df_fixes = self.df.copy()
        filtering_mask = ((df_fixes['issue_type'] == 'invalid_id') &
                          (df_fixes['correct_participant_identifier'].isna()))

        # |
        # (df_fixes['correct_participant_identifier'] == '')))

        def run_id_correction(idx):
            print(f"applying ids correction for {idx}")

            if isinstance(idx, str):
                if VALID_PATTERN_USING_MINUS.match(idx) or VALID_PATTERN_USING_UNDERSCORE.match(idx):
                    pass

                elif idx.startswith("I") and not re.search(r'[_\-—]', idx):
                    match = re.match(r'^I([A-Z]{2})(P)(\d+)$', idx)
                    if match:
                        return f"I-{match.group(1)}-{match.group(2)}-{match.group(3)}"

                elif idx.startswith("I"):
                    match1 = re.match(r'^I-([A-Z]{2})-([A-Z])(\d+)$', idx)
                    match2 = re.match(r'^I_([A-Z]{2})_([A-Z])(\d+)$', idx)
                    match3 = re.match(r'^I([A-Z]{2})([A-Z])(\d+)$', idx)

                    if match1:
                        print(f"Match {idx} for {match1.group(1)}")
                        return f"I_{match1.group(1)}_{match1.group(2)}_{match1.group(3)}"
                    elif match2:
                        print(f"Match {idx} for {match2.group(1)}")
                        return f"I_{match2.group(1)}_{match2.group(2)}_{match2.group(3)}"
                    elif match3:
                        print(f"Match {idx} for {match2.group(1)}")
                        return f"I_{match3.group(1)}_{match3.group(2)}_{match3.group(3)}"
                else:
                    return ''

        df_fixes.loc[filtering_mask, 'correct_participant_identifier'] = (
            df_fixes.loc[filtering_mask, 'participant_identifier'].apply(run_id_correction))

        df_fixes['Action'] = df_fixes.apply(  # TODO: break down
            lambda row: (
                'switch' if row['issue_type'] == 'invalid_id' and pd.notna(row['correct_participant_identifier'])
                            and row['correct_participant_identifier'] != '' else 'delete' if row[
                            'issue_type'] == 'invalid_id' and (pd.isna(row['correct_participant_identifier']) or
                            row['correct_participant_identifier'] == '') else row['Action']), axis=1
            )

        # df_fixes.to_csv(os.path.join(fixes_path, f'second_fixes_{filename}'), index=False)
        # return df_fixes

    def ids_structure_correction(self, fixes_path, filename):

        print(f"\n\033[32mStarting cleaning process from '{filename}' \033[0m\n")

        self.ids_correction_for_movisensxs(fixes_path, filename)
        self.ids_correction_by_regex(fixes_path, filename)
