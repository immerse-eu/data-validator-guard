import os
import re
import warnings
import pandas as pd

from config.config_loader import load_config_file

warnings.simplefilter(action='ignore', category=UserWarning)

VALID_CENTER_ACRONYMS = ["BI", "LE", "MA", "WI", "BR", "KO", "CA", "LO"]
VALID_PARTICIPANT_TYPES = ["P", "C", "A"]
VALID_PATTERN_USING_MINUS = re.compile(r"^I-(" + "|".join(VALID_CENTER_ACRONYMS) + r")-(" + "|".join(VALID_PARTICIPANT_TYPES) + r")-\d{3}$")
VALID_PATTERN_USING_UNDERSCORE = re.compile(r"^I_(" + "|".join(["WI", "MA"]) + r")_(" + "|".join(VALID_PARTICIPANT_TYPES) + r")_\d{3}$")
VALID_ESM_IDS_PATH = load_config_file('auxiliarFiles', 'ids_reference_esm')


def create_merged_esm_ids_rulebook():
    reference_all_ids = []

    for file in os.listdir(VALID_ESM_IDS_PATH):
        print(f"Processing file {file}")

        if file.endswith(".xlsx") and not '~' in file:
            df = pd.read_excel(os.path.join(VALID_ESM_IDS_PATH, file))
            df_filter = df[['participant_id', 'participant_movi_nr', 'SiteCode', 'study_ID (MaganaMed)']]
            df_filter = df_filter[df_filter['participant_id'] != "example"]
            reference_all_ids.append(df_filter)

    all_ids_ref_df = pd.concat(reference_all_ids)
    duplicates = all_ids_ref_df[all_ids_ref_df.duplicated()]
    print("Numer duplicates found:", len(duplicates), " duplicates:\n", duplicates)
    # duplicates.to_excel(os.path.join(VALID_ESM_IDS_PATH, "duplicated_ids_reference.xlsx"), index=False)
    filter_all_ids = all_ids_ref_df.drop_duplicates()
    filter_all_ids['Action'] = filter_all_ids['study_ID (MaganaMed)'].apply(
        lambda x: "delete" if " " in str(x) or "delete" in str(x) or "TEST" in str(x) else (
            "update" if str(x).strip() != "" else ""))
    # filter_all_ids.to_excel(os.path.join(VALID_ESM_IDS_PATH, "new_merged_esm_ids_rulebook.xlsx"), index=False)
    return filter_all_ids


class DataCleaning:

    def __init__(self, df):
        self.df = df
        self.changes_df = df.copy()
        self.clean_df = df.copy()

        self.delete_ids = set()
        self.merge_ids = {}
        self.add_ids = {}
        self.update_ids = {}
        self.assign_id_to_T0 = set()
        self.assign_id_to_T1 = set()
        self.assign_id_to_T2 = set()
        self.assign_id_to_T3 = set()

    '''
       This function (changes_to_apply_when_using_rulebook) is in charge to apply the changes using first "rulebook"
       since "changes_to_apply_XX" only contain existing IDS with detected issues and not the "NEW" ids to add.
    '''
    def changes_to_apply_when_using_rulebook(self, rulebook):

        self.changes_df.copy()

        for _, row in rulebook.iterrows():
            participant_identifier = row['participant_identifier']
            participant_number = row['participant_number']
            correct_participant_identifier = row.get('correct_participant_identifier')

            # participant_identifier = row['participant_id']
            # participant_number = row['participant_movi_nr']
            # correct_participant_identifier = row.get('study_ID (MaganaMed)')

            action = str(row['action']).strip()
            key = (participant_identifier, participant_number)

            if action == 'delete':
                self.delete_ids.add(key)

            if action.startswith('add'):
                self.add_ids[key] = correct_participant_identifier

            if action.startswith('skip'):
                continue

            if action.startswith('use'):    # TODO:Fix merging
                continue
                # if "T0" in action:
                #     self.assign_id_to_T0[key].add(correct_participant_identifier)
                # if "T1" in action:
                #     self.assign_id_to_T1[key].add(correct_participant_identifier)
                # if "T2" in action:
                #     self.assign_id_to_T2[key].add(correct_participant_identifier)
                # if "T3" in action:
                #     self.assign_id_to_T3[key].add(correct_participant_identifier)

            if action.startswith('update'):
                self.update_ids[key] = correct_participant_identifier

            if action.startswith('merge'):  # TODO: Check correctness of merging
                # continue
                extract_participant_number_to_merge = action.split("merge")[1].strip()
                if "-" in extract_participant_number_to_merge:
                    participant_numbers_to_merge = extract_participant_number_to_merge.split("-")
                    for part_number in participant_numbers_to_merge:
                        self.merge_ids[part_number.strip()] = correct_participant_identifier
                        print('extract_participant_number_to_merge', part_number, correct_participant_identifier)


    def _apply_changes_from_esm_rulebook(self, current_df, participant_id_label, participant_num_label, filename):
        current_immerse_df = current_df.copy()
        # current_immerse_df[participant_id_label] = current_immerse_df[participant_id_label].fillna(method='ffill')
        current_immerse_df['correct_participant_id'] = current_immerse_df[participant_id_label]


        # Case 1: Deletion IDs
        if self.delete_ids:
            # print('IDs to delete: ', self.delete_ids)
            current_immerse_df = current_immerse_df[~current_immerse_df.apply(
                lambda row: (str(row[participant_id_label]), row[participant_num_label],) in self.delete_ids, axis=1)]

        # Case 2: Merging IDs
        if self.merge_ids:
            print("IDs to Merge: ", self.merge_ids)
            current_immerse_df['correct_participant_id'] = current_immerse_df.apply(
                lambda row: self.merge_ids.get(row[participant_num_label], row['correct_participant_id']), axis=1)

        # Case 3: Adding IDS
        if self.add_ids:
            # print("IDs to Add: ", self.add_ids)
            current_immerse_df['correct_participant_id'] = current_immerse_df.apply(
                lambda row: self.add_ids.get(row[participant_num_label], row['correct_participant_id'])
                if pd.isna(row['correct_participant_id']) or str(row['correct_participant_id']).strip() == ""
                else row['correct_participant_id'], axis=1)

        # Case 4: Update IDS
        if self.update_ids:
            # print("IDs to update: ", self.update_ids)
            current_immerse_df['correct_participant_id'] = current_immerse_df.apply(
            lambda row: self.update_ids.get((str(row[participant_id_label]), row[participant_num_label]), row['correct_participant_id']), axis=1)

        # Case 5: Specific IDs according T-files
        if '_T0_' in filename and self.assign_id_to_T0:
            # print("T0 IDs: ", self.assign_id_to_T0)
            current_immerse_df[participant_id_label] = current_immerse_df.apply(
                lambda row: self.assign_id_to_T0.get((row[participant_id_label], row[participant_num_label]), row[participant_id_label]), axis=1)

        if '_T1_' in filename and self.assign_id_to_T1:
            print("T1 IDs: ", self.assign_id_to_T1)
            current_immerse_df[participant_id_label] = current_immerse_df.apply(
                lambda row: self.assign_id_to_T1.get((row[participant_id_label], row[participant_num_label]), row[participant_id_label]), axis=1)

        if '_T2_' in filename and self.assign_id_to_T2:
            print("T2 IDs: ", self.assign_id_to_T2)
            current_immerse_df[participant_id_label] = current_immerse_df.apply(
                lambda row: self.assign_id_to_T2.get((row[participant_id_label], row[participant_num_label]), row[participant_id_label]), axis=1)

        if '_T3_' in filename and self.assign_id_to_T3:
            print("T3 IDs: ", self.assign_id_to_T3)
            current_immerse_df[participant_id_label] = current_immerse_df.apply(
                lambda row: self.assign_id_to_T3.get((row[participant_id_label], row[participant_num_label]), row[participant_id_label]), axis=1)

        current_immerse_df[participant_id_label] = current_immerse_df.pop("correct_participant_id")
        return current_immerse_df

    # Changes to apply to ORIGINAL_IMMERSE_SOURCE
    def execute_corrections_to_original_tables(self, original_directory: str, immerse_system):
        # df_issues = self.df.copy()

        immerse_clean_dfs = {}
        files_to_exclude = ["Sensing.xlsx", "codebook.xlsx", "~$IMMERSE_T0_BE.xlsx",
                            "Fidelity_BE.xlsx", "Fidelity_c_UK.xlsx", "Fidelity_GE.xlsx", "Fidelity_SK.xlsx",
                            "Fidelity_UK.xlsx", "IMMERSE_Fidelity_SK_Kosice.xlsx"]

        for root, dirs, files in os.walk(original_directory):
            if immerse_system in dirs:
                sub_folder_path = os.path.join(root, immerse_system)
                for folder, _, files in os.walk(sub_folder_path):
                    for filename in files:
                        if filename in files_to_exclude:  # These files use another labeling
                            continue
                        if filename.endswith(".xlsx"):
                            filepath = os.path.join(folder, filename)
                            try:
                                if 'movisens_sensing' == immerse_system and 'movisens_sensing' in filepath:
                                    print(f"Processing  df from {filename}")
                                    current_df = pd.read_excel(filepath, engine='openpyxl') if filename.endswith(".xlsx") else pd.read_csv(filepath)
                                    clean_current_df = self._apply_changes_from_esm_rulebook(current_df, "study_id", "Participant", filename)
                                    filename = os.path.join(sub_folder_path, f"_{filename}")
                                    clean_current_df.to_excel(filename, index=False)
                                    print(f"Cleaned {filename} successfully exported")

                            except Exception as e:
                                print(f"Unexpected error in  {filename}", e)

        return immerse_clean_dfs

    def issues_to_correct_from_esm_rulebook(self, esm_rulebook, fixes_path, filename):
        df_issues = self.df.copy()
        merged_esm_ids_rulebook_df = esm_rulebook
        merged_esm_ids_rulebook_df.rename(columns={merged_esm_ids_rulebook_df.columns[0]: 'participant_identifier'},
                                          inplace=True)
        merged_esm_ids_rulebook_df.rename(columns={merged_esm_ids_rulebook_df.columns[1]: 'participant_number'},
                                          inplace=True)
        merged_esm_ids_rulebook_df.rename(
            columns={merged_esm_ids_rulebook_df.columns[3]: 'correct_participant_identifier'}, inplace=True)

        df_issues['participant_identifier'] = df_issues['participant_identifier'].astype(str)
        merged_esm_ids_rulebook_df['participant_identifier'] = merged_esm_ids_rulebook_df[
            'participant_identifier'].astype(str)

        self.changes_df = pd.merge(df_issues, merged_esm_ids_rulebook_df, on='participant_identifier', how='inner')
        # self.changes_df.to_csv(os.path.join(fixes_path, f'updated_changes_{filename}'), index=False)
        return self.changes_df

    def ids_correction_by_regex(self, fixes_path, filename):
        df_fixes = self.clean_df.copy()
        print(df_fixes.head())
        filtering_mask = ((df_fixes['issue_type'] == 'invalid_id') &
                          (df_fixes['correct_participant_identifier'].isna()))

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
                            and row['correct_participant_identifier'] != '' else 'delete'
                if row['issue_type'] == 'invalid_id' and (pd.isna(row['correct_participant_identifier'])
                or row['correct_participant_identifier'] == '') else row['Action']), axis=1)

        df_fixes.to_csv(os.path.join(fixes_path, f'second_fixes_{filename}'), index=False)
        return df_fixes

    '''
    NOTE: 
    
    The following method requires 2 input sources to clean ids: 
    - esm_rulebook: Contains all ESM ids and their correct id (here adds more ids).
    - changes_path: Contains only those ids which were found in original data. 
    
    '''

    def prepare_ids_correction_from_esm(self, esm_rulebook, changes_path, filename):
        print(f"\n\033[32mStarting cleaning process from '{filename}' \033[0m\n")

        # TODO: Define changes only for MovisensESM
        if "movisens" in filename:
            self.changes_df = self.issues_to_correct_from_esm_rulebook(esm_rulebook, changes_path, filename)
            return self.changes_df
        # self.execute_corrections_to_original_tables(original_source_path, esm_rulebook)
        # self.execute_corrections_to_original_tables(original_source_path)

        # self.changes_df = self.ids_correction_by_regex(changes_path, filename)
        # print(self.clean_df)
