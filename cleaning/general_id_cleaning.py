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
    duplicates.to_excel(os.path.join(VALID_ESM_IDS_PATH, "duplicated_ids_reference.xlsx"), index=False)
    filter_all_ids = all_ids_ref_df.drop_duplicates()
    filter_all_ids['Action'] = filter_all_ids['study_ID (MaganaMed)'].apply(
        lambda x: "delete" if " " in str(x) or "delete" in str(x) or "TEST" in str(x) else "")
    filter_all_ids.to_excel(os.path.join(VALID_ESM_IDS_PATH, "new_merged_esm_ids_rulebook.xlsx"), index=False)
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
        self.assign_id_to_T1 = set()
        self.assign_id_to_T2 = set()
        self.assign_id_to_T3 = set()

    def changes_to_apply_when_using_rulebook(self, rulebook):
        # self.changes_df.copy()
        # print(rulebook.info())

        for _, row in rulebook.iterrows():
            participant_identifier = row['participant_identifier']
            participant_number = row['participant_number']
            correct_participant_identifier = row.get('correct_participant_identifier')

            action = str(row['action']).strip()
            key = (participant_identifier, participant_number)

            if action == 'delete':
                print("IDs to delete:", key)
                self.delete_ids.add(key)

            elif action.startswith('merge'):
                # print("IDs to merge:", key, "action:", action)
                extract_participant_number_to_merge = action.split("merge")[1]
                if len(extract_participant_number_to_merge) > 2:
                    get_participant_numbers = extract_participant_number_to_merge.split("-")
                    print(key, get_participant_numbers)

                    print('extract_participant_number_to_merge', extract_participant_number_to_merge)
                    # self.merge_ids[key] = correct_participant_identifier   # TODO: review correctness of merging
                    # self.merge_ids[participant_num] = action.split('merge')[-1].strip() # TODO: review merging

            elif action.startswith('add'):
                self.add_ids[participant_number] = correct_participant_identifier
            elif action.startswith('use'):
                if "T1" in action: self.assign_id_to_T1[key].add(correct_participant_identifier)    # TODO: review merging
                if "T2" in action: self.assign_id_to_T2.add(key)
                if "T3" in action: self.assign_id_to_T3.add(key)
            else:
                self.update_ids[key] = correct_participant_identifier

    def _apply_changes_from_esm_rulebook(self, current_df):
        current_immerse_df = current_df.copy()

        # Column Names' homologation.
        if 'id' in current_immerse_df.columns:
            current_immerse_df.rename(columns={current_immerse_df['id']: 'participant_identifier'}, inplace=True)
        if 'participant_id' in current_immerse_df.columns:
            current_immerse_df.rename(columns={current_immerse_df['id']: 'participant_identifier'}, inplace=True)
        current_immerse_df.rename(columns={current_immerse_df['Participant']: 'participant_number'}, inplace=True)

        # Case 1: Deletion
        current_immerse_df = current_immerse_df[~current_immerse_df.apply(
            lambda row: (row['participant_identifier'], row['participant_number']) in self.delete_ids, axis=1)]

        # Case 2: Merging
        current_immerse_df['participant_identifier'] = current_immerse_df.apply(
            lambda row: self.merge_ids.get(row['participant_number'], row['participant_identifier']), axis=1)

        # Case 3: Adding
        current_immerse_df['participant_identifier'] = current_immerse_df.apply(
            lambda row: self.add_ids.get(row['participant_number'], row['participant_identifier'])
            if pd.isna(row['participant_identifier']) else row['participant_identifier'],axis=1)

        # TODO: Include Update IDs!

    def execute_corrections_to_original_tables(self, original_directory: str):
        df_issues = self.df.copy()
        immerse_clean_dfs = {}
        files_to_exclude = ["Sensing.xlsx", "codebook.xlsx", "~$IMMERSE_T0_BE.xlsx"]

        for folder, _, files in os.walk(original_directory):
            sub_folder_name = os.path.basename(folder)
            immerse_clean_dfs[sub_folder_name] = {}

            for filename in files:
                if not filename.endswith(".xlsx") or filename.endswith(".csv"):
                    continue
                if filename in files_to_exclude:   # These files use another labeling
                    continue
                filepath = os.path.join(folder, filename)
                try:
                    current_df = pd.read_excel(filepath, engine='openpyxl') if filename.endswith(".xlsx") else pd.read_csv(filepath)
                    # print(f"Processing  df from {filename}")
                    clean_df = self._apply_changes_from_esm_rulebook(current_df)
                    # immerse_clean_dfs[folder_name][filename] = clean_df
                except Exception as e:
                    print(f"Unexpected error in  {filename}", e)

        return immerse_clean_dfs

    def issues_to_correct_from_esm_rulebook(self, esm_rulebook, fixes_path, filename):
        df_issues = self.df.copy()
        merged_esm_ids_rulebook_df = esm_rulebook
        merged_esm_ids_rulebook_df.rename(columns={merged_esm_ids_rulebook_df.columns[0]: 'participant_identifier'}, inplace=True)
        merged_esm_ids_rulebook_df.rename(columns={merged_esm_ids_rulebook_df.columns[1]: 'participant_number'}, inplace=True)
        merged_esm_ids_rulebook_df.rename(columns={merged_esm_ids_rulebook_df.columns[3]: 'correct_participant_identifier'}, inplace=True)

        df_issues['participant_identifier'] = df_issues['participant_identifier'].astype(str)
        merged_esm_ids_rulebook_df['participant_identifier'] = merged_esm_ids_rulebook_df[
            'participant_identifier'].astype(str)

        self.changes_df = pd.merge(df_issues, merged_esm_ids_rulebook_df, on='participant_identifier', how='inner')
        # self.clean_df.to_csv(os.path.join(fixes_path, f'changes_{filename}'), index=False)
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
                            and row['correct_participant_identifier'] != '' else 'delete' if row[
                            'issue_type'] == 'invalid_id' and (
                            pd.isna(row['correct_participant_identifier']) or
                            row['correct_participant_identifier'] == '') else
                            row['Action']), axis=1
                )

        df_fixes.to_csv(os.path.join(fixes_path, f'second_fixes_{filename}'), index=False)
        return df_fixes

    '''
    NOTE: 
    
    The following method requires 2 input sources to clean ids: 
    - esm_rulebook: Contains all ESM ids and their correct id (here adds more ids).
    - changes_path: Contains only those ids which were found in original data. 
    
    '''
    def prepare_ids_correction_from_esm(self, esm_rulebook, changes_path, original_source_path, filename):
        print(f"\n\033[32mStarting cleaning process from '{filename}' \033[0m\n")

        self.changes_df = self.issues_to_correct_from_esm_rulebook(esm_rulebook, changes_path, filename)
        return self.changes_df
        # self.execute_corrections_to_original_tables(original_source_path, esm_rulebook)
        # self.execute_corrections_to_original_tables(original_source_path)

        # self.changes_df = self.ids_correction_by_regex(changes_path, filename)
        # print(self.clean_df)
