import os
import re
import pandas as pd

from config.config_loader import load_config_file

VALID_CENTER_ACRONYMS = ["BI", "LE", "MA", "WI", "BR", "KO", "CA", "LO"]
VALID_PARTICIPANT_TYPES = ["P", "C", "A"]

VALID_PATTERN_USING_MINUS = re.compile(
    r"^I-(" + "|".join(VALID_CENTER_ACRONYMS) + r")-(" + "|".join(VALID_PARTICIPANT_TYPES) + r")-\d{3}$")
VALID_PATTERN_USING_UNDERSCORE = re.compile(
    r"^I_(" + "|".join(["WI", "MA"]) + r")_(" + "|".join(VALID_PARTICIPANT_TYPES) + r")_\d{3}$")

VALID_ESM_IDS_PATH = load_config_file('auxiliarFiles', 'ids_reference_esm')


class DataCleaning:

    def __init__(self, df):
        self.df = df
        self.clean_df = df

    def ids_correction_for_movisensxs(self):
        id_correction = {}
        for file in os.listdir(VALID_ESM_IDS_PATH):
            if file.endswith(".xlsx") and not '~' in file:
                df = pd.read_excel(os.path.join(VALID_ESM_IDS_PATH, file))
                df_filter = df[['participant_id', 'study_ID (MaganaMed)']]
                print(df_filter)

    def ids_structure_correction(self, id_column, filename):
        valid_ids = []
        ids_to_correct = {}

        self.clean_df = self.df.copy()

        if 'movisens' in filename:
            # TODO: Use all ESM from Anita
            self.ids_correction_for_movisensxs()
        else:
            for idx in self.clean_df[id_column]:
                if isinstance(idx, str):
                    if VALID_PATTERN_USING_MINUS.match(idx) or VALID_PATTERN_USING_UNDERSCORE.match(idx):
                        valid_ids.append(idx)
                        continue
                    elif idx.startswith("I") and not re.search(r'[_\-—]', idx):
                        potential_match = re.match(r'^I([A-Z]{2})(P)(\d+)$', idx)

                        if potential_match:
                            acronym_part = potential_match.group(1)
                            p_type_part = potential_match.group(2)
                            number_part = potential_match.group(3)

                            corrected_id = f"I-{acronym_part}-{p_type_part}-{number_part}"
                            if idx not in ids_to_correct:
                                ids_to_correct[idx] = corrected_id
                    elif idx.startswith("I") and " ":
                        potential_case_match_one = re.match(r'^I-([A-Z]{2})-([A-Z])(\d+)$', idx)
                        potential_case_match_two = re.match(r'^I([A-Z]{2})([A-Z])(\d+)$', idx)

                        if potential_case_match_one:
                            acronym_part = potential_case_match_one.group(1)
                            p_type_part = potential_case_match_one.group(2)
                            number_part = potential_case_match_one.group(3)
                            corrected_id = f"I_{acronym_part}_{p_type_part}_{number_part}"
                            if idx not in ids_to_correct:
                                ids_to_correct[idx] = corrected_id

                        elif potential_case_match_two:
                            acronym_part = potential_case_match_two.group(1)
                            p_type_part = potential_case_match_two.group(2)
                            number_part = potential_case_match_two.group(3)
                            corrected_id = f"I_{acronym_part}_{p_type_part}_{number_part}"
                            if idx not in ids_to_correct:
                                ids_to_correct[idx] = corrected_id

            print(ids_to_correct)
