import csv
import os
import warnings
import pandas as pd
from pathlib import Path
from utils.rulebook import get_columns_from_id_reference
from utils.auxiliar_functions import read_all_dataframes

warnings.simplefilter(action='ignore', category=UserWarning)

# files that have a different structure or do not have participants' IDs, but clinicians IDs.
files_to_exclude = ["Sensing.xlsx", "codebook.xlsx", "~$IMMERSE_T0_BE.xlsx",
                    "Fidelity_BE.xlsx", "Fidelity_c_UK.xlsx", "Fidelity_GE.xlsx", "Fidelity_SK.xlsx",
                    "Fidelity_UK.xlsx", "IMMERSE_Fidelity_SK_Kosice.xlsx",
                    "Service-characteristics-(Teamleads).csv",
                    "Service-characteristics.csv", "ORCA.csv"]

system_configs = {
    'maganamed': {
        'column_id': 'participant_identifier',
        'column_id_number': None,
        'folder': 'cleaned_ids_maganamed',
    },
    'movisens_esm': {
        'column_id': 'participant_identifier',
        'column_id_number': 'participant_number',
        'folder': 'cleaned_ids_movisens_esm',
    },
    'movisens_esm_fidelity': {
        'column_id': 'participant_id',
        'column_id_number': 'participant_movi_nr',
        'folder': 'cleaned_ids_movisens_esm_fidelity',
    },
    'movisens_sensing': {
        'column_id': 'study_id',
        'column_id_number': 'participant',
        'folder': 'cleaned_ids_movisens_sensing',
    },
    'dmmh': {
        'column_id': 'study_id',
        'column_id_number': 'participant',
        'folder': 'cleaned_ids_dmmh',
    },
}


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

    # Step 1: Define changes to apply from rulebook
    # This function (changes_to_apply_when_using_rulebook) applies changes in IDS using a "rulebook"
    # to the existing IDS.
    def changes_to_apply_when_using_rulebook(self, rulebook, system):

        self.changes_df.copy()

        for _, row in rulebook.iterrows():
            participant_identifier = row['participant_identifier']
            correct_participant_identifier = row.get('correct_participant_identifier')
            action = str(row['action']).strip()

            # This section defines the type of keys, ESM requires 4 values, while Magana just one key.
            # Note: To apply these changes, column SiteCode and VisitCode must already exist in the data.
            # In addition, "participant_identifier" will be not used since there are some missing values.
            if "esm" in system:
                participant_number = row['participant_number']
                visit_code = row['VisitCode']
                site_code = row['SiteCode']
                if participant_identifier:
                    key = (participant_identifier, participant_number, visit_code, site_code)
                if not site_code and participant_identifier:
                    key = (participant_number, visit_code)

            elif "maganamed" in system:
                key = participant_identifier
            else:
                key = None

            # This section identifies those IDs which will require to apply changes according to each type of action.
            if action == 'delete' and key is not None:
                self.delete_ids.add(key)

            if action.startswith('add') and key is not None:
                self.add_ids[key] = correct_participant_identifier

            if action.startswith('skip') and key is not None:
                continue

            if action.startswith('use') and key is not None:  # TODO:Fix merging
                continue
                # if "T0" in action:
                #     self.assign_id_to_T0[key] = correct_participant_identifier
                # if "T1" in action:
                #     self.assign_id_to_T1[key] = correct_participant_identifier
                # if "T2" in action:
                #     self.assign_id_to_T2[key] = correct_participant_identifier
                # if "T3" in action:
                #     self.assign_id_to_T3[key] = correct_participant_identifier

            if action.startswith('update') and key is not None:
                if system == 'maganamed':
                    self.update_ids[key] = {
                        'correct_participant_identifier': correct_participant_identifier,
                        'unit': row.get('unit'),
                        'condition': row.get('condition'),
                        'randomize': row.get('randomize')
                    }
                else:
                    self.update_ids[key] = correct_participant_identifier

            if action.startswith('merge') and key is not None:  # TODO: Check correctness of merging
                self.merge_ids[key] = correct_participant_identifier
                continue
                # extract_participant_number_to_merge = action.split("merge")[1].strip()
                # if "-" in extract_participant_number_to_merge:
                #     participant_numbers_to_merge = extract_participant_number_to_merge.split("-")
                #     for part_number in participant_numbers_to_merge:
                #         self.merge_ids[part_number.strip()] = correct_participant_identifier
                #         print('extract_participant_number_to_merge', part_number, correct_participant_identifier)

        print(f"{len(self.delete_ids)} ids to delete:", self.delete_ids,
              f"\n{len(self.update_ids)} ids to update: ", self.update_ids,
              f"\n{len(self.add_ids)} ids to add: ", self.add_ids,
              f"\n{len(self.merge_ids)} ids to merge: ", self.merge_ids)

    # Step 2. Apply changes from rulebook
    def _apply_changes_from_rulebook(self, current_df, participant_identifier, participant_number, filename, system):
        current_immerse_df = current_df.copy()
        current_immerse_df['correct_participant_id'] = current_immerse_df[participant_identifier]

        # Case 1: Deletion IDs
        if self.delete_ids:
            print('IDs to delete: ', self.delete_ids)
            if "movisens_esm" in system:
                # TODO: Verify functionality
                current_immerse_df = current_immerse_df[~current_immerse_df.apply(
                    lambda row: (row[participant_identifier],
                                 row[participant_number],
                                 row['VisitCode'],
                                 row['SiteCode']) in self.delete_ids, axis=1)]

            else:
                current_immerse_df = current_immerse_df[~current_immerse_df.apply(
                    lambda row: (str(row[participant_identifier])) in self.delete_ids, axis=1)]

        # # Case 2: Merging IDs
        if self.merge_ids:
            print("IDs to Merge: ", self.merge_ids)
            if "movisens_esm" in system:
                current_immerse_df['correct_participant_id'] = current_immerse_df.apply(
                    lambda row: self.merge_ids.get(row[participant_number], row['correct_participant_id']), axis=1)
            else:
                current_immerse_df['correct_participant_id'] = current_immerse_df.apply(
                    lambda row: self.merge_ids.get(row[participant_identifier], row['correct_participant_id']), axis=1)

        # Case 3: Adding IDS
        if self.add_ids:
            print("IDs to Add: ", self.add_ids)
            if "movisens_esm" in system:

                normalize_ids = {
                    tuple(str(x).strip() for x in k): value
                    for k, value in self.add_ids.items()
                }

                def lookup_row(row):
                    key = (
                        str(row[participant_identifier]).strip(),
                        str(row[participant_number]).strip(),
                        str(row['VisitCode']).strip(),
                        str(row['SiteCode']).strip()
                    )
                    return normalize_ids.get(key, row.get('correct_participant_id'))

                current_immerse_df['correct_participant_id'] = current_immerse_df.apply(lookup_row, axis=1)
            # current_immerse_df['correct_participant_id'] = current_immerse_df.apply(
            #     lambda row: self.add_ids.get(row[participant_number], row['correct_participant_id'])
            #     if pd.isna(row['correct_participant_id']) or str(row['correct_participant_id']).strip() == ""
            #     else row['correct_participant_id'], axis=1)
        #
        # Case 4: Update IDS
        if self.update_ids:
            if "movisens_esm" in system:
                print("IDs to update : ", self.update_ids)

                normalize_ids = {
                    tuple(str(x).strip() for x in k): value
                    for k, value in self.update_ids.items()
                }

                def lookup_row(row):
                    key = (
                        str(row[participant_identifier]).strip(),
                        str(row[participant_number]).strip(),
                        str(row['VisitCode']).strip(),
                        str(row['SiteCode']).strip()
                    )
                    return normalize_ids.get(key, row.get('correct_participant_id'))
                current_immerse_df['correct_participant_id'] = current_immerse_df.apply(lookup_row, axis=1)

            # Maganamed
            # current_immerse_df['correct_participant_id'] = current_immerse_df.apply(
            #     lambda row: self.update_ids.get((str(row[participant_identifier]), row[participant_number]),
            #                                     row['correct_participant_id']), axis=1)

            # Extended update include cases where values for "unit, "condition" or "randomize" values are missing.
            def apply_extended_update_id(row):
                original_id = str(row[participant_identifier])
                if original_id in self.update_ids:
                    update_row = self.update_ids[original_id]
                    print('original_id', original_id, "update_id", update_row)
                    row['correct_participant_id'] = update_row['correct_participant_identifier']

                    for col in ['unit', 'condition', 'randomize']:
                        value = row.get(col)
                        if pd.isna(value) or str(value).strip() == '':
                            update_value = update_row.get(col)
                            if update_value is not None:
                                row[col] = update_value
                                # print('update_value', row[col], "for participant", row['correct_participant_id'])
                return row

            if "maganamed" in system:
                current_immerse_df = current_immerse_df.apply(apply_extended_update_id, axis=1)

        # Case 5: Specific IDs according T-files
        if '_T0_' in filename and self.assign_id_to_T0:
            # print("T0 IDs: ", self.assign_id_to_T0)
            current_immerse_df[participant_identifier] = current_immerse_df.apply(
                lambda row: self.assign_id_to_T0.get((row[participant_identifier], row[participant_number]),
                                                     row[participant_identifier]), axis=1)

        if '_T1_' in filename and self.assign_id_to_T1:
            print("T1 IDs: ", self.assign_id_to_T1)
            current_immerse_df[participant_identifier] = current_immerse_df.apply(
                lambda row: self.assign_id_to_T1.get((row[participant_identifier], row[participant_number]),
                                                     row[participant_identifier]), axis=1)

        if '_T2_' in filename and self.assign_id_to_T2:
            print("T2 IDs: ", self.assign_id_to_T2)
            current_immerse_df[participant_identifier] = current_immerse_df.apply(
                lambda row: self.assign_id_to_T2.get((row[participant_identifier], row[participant_number]),
                                                     row[participant_identifier]), axis=1)

        if '_T3_' in filename and self.assign_id_to_T3:
            print("T3 IDs: ", self.assign_id_to_T3)
            current_immerse_df[participant_identifier] = current_immerse_df.apply(
                lambda row: self.assign_id_to_T3.get((row[participant_identifier], row[participant_number]),
                                                     row[participant_identifier]), axis=1)

        current_immerse_df[participant_identifier] = current_immerse_df.pop("correct_participant_id")
        return current_immerse_df

    # Step 3. Complete ALL ids which 'unit', 'condition', 'randomize' are missing.
    def add_unit_site_and_randomized_values(self, cleand_df, id_column):
        reference_df = get_columns_from_id_reference()
        ref_lookup = reference_df.set_index('correct_participant_identifier')
        [['unit', 'condition', 'randomize']].to_dict(orient='index')

        def update_row(row):
            original_id = str(row[id_column])
            if original_id in ref_lookup:
                update_info = ref_lookup[original_id]
                for col in ['unit', 'condition', 'randomize']:
                    if pd.isna(row.get(col)) or str(row.get(col)).strip() == '':
                        row[col] = update_info.get(col)
            return row

        updated_df = cleand_df.apply(update_row, axis=1)
        return updated_df

    # Step 4.  Changes to apply to ORIGINAL_IMMERSE_SOURCE
    def execute_corrections_to_original_tables(self, original_directory: str, immerse_system):
        immerse_clean_dfs = {}
        config = system_configs.get(immerse_system)

        if not config:
            print(f"No config found for system: {immerse_system}")

        dataframes, filenames = read_all_dataframes(original_directory, immerse_system)
        if len(filenames) == len(dataframes):
            filenames_and_dataframes = list(zip(filenames, dataframes))

            # Output folder
            cleaned_folder = Path(original_directory) / immerse_system / config['folder']
            cleaned_folder.mkdir(parents=True, exist_ok=True)

            for filename, dataframe in filenames_and_dataframes:
                print(f"\nProcessing {filename}...")
                try:
                    df = self._apply_changes_from_rulebook(
                        dataframe,
                        config['column_id'],
                        config['column_id_number'],
                        filename,
                        immerse_system)

                    if config['column_id']:
                        df.rename(columns={config['column_id']: "participant_identifier"}, inplace=True)
                    if config['column_id_number']:
                        df.rename(columns={config['column_id_number']: "participant_number"}, inplace=True)

                    # Add additional cleaning if necessary
                    if 'maganamed' in immerse_system:
                        df = self.add_unit_site_and_randomized_values(df, "participant_identifier")

                    # Save all files as CSV with ";"
                    output_path = cleaned_folder / filename.replace(".xlsx", ".csv")
                    df.to_csv(output_path, sep=";", index=False)

                    print(f"Exported cleaned file to {output_path}")
                    immerse_clean_dfs[filename] = df

                except Exception as e:
                    print(f"Execute corrections function. Unexpected error in {filename}: {e}")

            return immerse_clean_dfs

    def issues_to_correct_from_esm_rulebook(self, esm_rulebook, fixes_path, filename):
        '''
        In this step, rename columns in esm_rulebook to guarantee columns name across different rulebooks.
        In addition, detected issues are merged with the rulebook to create a merged filed with the specific changes
        that should be carried out.
        '''

        df_issues = self.df.copy()
        merged_esm_ids_rulebook_df = esm_rulebook

        merged_esm_ids_rulebook_df.rename(
            columns={merged_esm_ids_rulebook_df.columns[0]: 'participant_identifier'}, inplace=True)
        merged_esm_ids_rulebook_df.rename(
            columns={merged_esm_ids_rulebook_df.columns[1]: 'participant_number'}, inplace=True)
        merged_esm_ids_rulebook_df.rename(
            columns={merged_esm_ids_rulebook_df.columns[5]: 'correct_participant_identifier'},
            inplace=True)

        merged_esm_ids_rulebook_df['action'] = merged_esm_ids_rulebook_df['action'].astype(str)

        # Change float to int values
        if 'VisitCode' in merged_esm_ids_rulebook_df.columns:
            merged_esm_ids_rulebook_df['VisitCode'] = merged_esm_ids_rulebook_df['VisitCode'].apply(
                lambda x: int(x) if isinstance(x, float) and not pd.isnull(x) else '')

        # # Change float to int values
        if 'SiteCode' in merged_esm_ids_rulebook_df.columns:
            merged_esm_ids_rulebook_df['SiteCode'] = merged_esm_ids_rulebook_df['SiteCode'].apply(
                lambda x: int(x) if isinstance(x, float) and not pd.isnull(x) else x)

        # Option 1: Use a copy of the rulebook with the new column naming convention.
        updated_rulebook_df = merged_esm_ids_rulebook_df.copy()

        # Option 2: Merging issues with rulebook to define and export changes.csv
        df_issues['participant_identifier'] = df_issues['participant_identifier']
        merged_esm_ids_rulebook_df['participant_identifier'] = merged_esm_ids_rulebook_df[
            'participant_identifier'].astype(str)
        self.changes_df = pd.merge(df_issues, merged_esm_ids_rulebook_df, on='participant_identifier', how='inner')
        self.changes_df.to_csv(os.path.join(fixes_path, f'identified_id_issues_and_changes_extracted_ids_{filename}'), index=False)

        updated_rulebook_df.to_csv(os.path.join(fixes_path, f'updated_rulebook.csv'), index=False)
        return updated_rulebook_df

    # ---> Step 1 ESM
    def prepare_ids_correction_from_esm(self, esm_rulebook, changes_path, filename):
        '''
          The following method requires 2 input sources to clean ids:
          - esm_rulebook: Contains all ESM ids and their correct id (here adds more ids).
          - changes_path: Contains only those ids which were found in original data.
        '''

        print(f"\n\033[32mStarting cleaning process from '{filename}' \033[0m\n")
        if "movisens_esm" in filename:
            new_esm_rulebook = self.issues_to_correct_from_esm_rulebook(esm_rulebook, changes_path, filename)
            return new_esm_rulebook

