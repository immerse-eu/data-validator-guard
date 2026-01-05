import os
import traceback
import warnings
from typing import Any, Optional, Union, Dict

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
    'movisens_fidelity': {
        'column_id': ['fidelity_idparticiant', 'time', 'patient_id', 'id', 'fidelity_t2_idparticiant', 'item_770'],
        'column_id_number': 'Participant',
        'folder': 'cleaned_ids_movisens_fidelity',
    },
    'movisens_sensing': {
        'column_id': 'study_id',
        'column_id_number': 'participant',
        'folder': 'cleaned_ids_movisens_sensing',
    },
    'dmmh': {
        'column_id': 'Participant',
        'column_id_number': None,
        'folder': 'cleaned_ids_dmmh',
    },
    'redcap': {
        'column_id': 'participant_identifier',   # Alternative: "record_id"
        'column_id_number': None,
        'folder': 'cleaned_ids_redcap',
    },
}

rename_fidelity_columns_dict = {
    'fidelity_idparticiant': 'fidelity_participant_identifier_T1',
    'patient_id': 'reported_patient_identifier',
    'id': 'rater_participant_identifier',
    'fidelity_t2_idparticiant': 'fidelity_participant_identifier_T2'
}


# Small helpers kept local and minimal to avoid changing external behaviour
def _normalize_key(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    return str(v).strip()


def _normalize_numberish(v):
    """Turn NaN->None, 1.0->1, numeric strings to numbers when possible, else stripped string."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, float):
        if abs(v - int(v)) < 1e-9:
            return int(v)
        return v
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        s = v.strip()
        try:
            f = float(s)
            if abs(f - int(f)) < 1e-9:
                return int(f)
            return f
        except Exception:
            return s
    return v


class DataCleaning:

    def __init__(self, df, debug: bool = False):
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
        self.debug = True

    # ---> Step 2.
    # This function identifies the KEYs to apply changes (add, delete, update, etc...) in original data.
    def changes_to_apply_when_using_rulebook(self, rulebook, system):

        '''
        This section defines the type of keys used. ESM requires 4 values, while the other systems require just one key.
        In addition, it clusters the IDs according to the defined 'Action' from rulebooks.
        ESM Note: To apply these changes, column 'SiteCode' and 'VisitCode' must already exist in the data!!
        ESM Note: "participant_identifier" will be not used as a primary KEY since there are some missing values.
        '''

        alternative_systems = ['maganamed', 'dmmh', 'redcap', 'movisens_fidelity']

        self.changes_df.copy()

        for _, row in rulebook.iterrows():
            participant_identifier = row['participant_identifier']
            correct_participant_identifier = row.get('correct_participant_identifier')
            action = str(row['action']).strip()

            # normalize keys immediately
            key_norm = _normalize_key(participant_identifier)

            if "esm" in system:
                participant_number = row.get('participant_number')
                visit_code = row.get('VisitCode')
                site_code = row.get('SiteCode')
                country = row.get('Country')

                # Normalized elements
                participant_number = _normalize_key(participant_number)
                visit_code = _normalize_key(visit_code)
                site_code = _normalize_key(site_code)

                if key_norm:
                    key = (key_norm, participant_number, visit_code, site_code)
                else:
                    key = (participant_number, country, visit_code)  # key for 'add' actions

            elif any(value in system for value in alternative_systems):
                key = key_norm
            else:
                key = None

            # This section identifies those IDs which will require to apply changes according to each type of action.
            if action == 'delete' and key is not None:
                self.delete_ids.add(key)
                continue

            if action.startswith('add') and key is not None:
                self.add_ids[key] = correct_participant_identifier
                continue

            if (action.startswith('skip') or action.startswith('check manually')) and key is not None:
                continue

            if action.startswith('use') and key is not None:  # TODO:Fix merging
                continue

            if action.startswith('update') and key is not None:
                '''
                Minimal targeted change: store structured dicts for maganamed so extended update
                can fill unit/condition/randomize safely.
                '''
                if system == 'maganamed':
                    self.update_ids[key] = {
                        'correct_participant_identifier': correct_participant_identifier,
                        'unit': row.get('unit'),
                        'condition': row.get('condition'),
                        'randomize': row.get('randomize')
                    }
                    # Also index under corrected id if present and different (tolerate variants)
                    corrected_norm = _normalize_key(correct_participant_identifier)
                    if corrected_norm and corrected_norm != key and corrected_norm not in self.update_ids:
                        self.update_ids[corrected_norm] = self.update_ids[key]
                else:
                    self.update_ids[key] = correct_participant_identifier
                continue

            if action.startswith('merge') and key is not None:  # TODO: Check correctness of merging
                self.merge_ids[key] = correct_participant_identifier
                continue

        if self.debug:
            print(f"DEBUG: "
                  f"delete_ids={len(self.delete_ids)}\n{self.delete_ids}"
                  f"\nupdate_ids={len(self.update_ids)}\n{self.update_ids} "
                  f"\nadd_ids={len(self.add_ids)}\n{self.add_ids}"
                  f"\nmerge_ids={len(self.merge_ids)}\n{self.merge_ids} ")

    # Apply changes from rulebook
    def _apply_changes_from_rulebook(self, current_df, participant_identifier, participant_number, filename, system):
        current_immerse_df = current_df.copy()

        primary_identifier = None
        if 'movisens_fidelity' in system:
            primary_identifier = participant_identifier
            if primary_identifier in current_df.columns:
                current_immerse_df['correct_participant_id'] = current_immerse_df[primary_identifier].where(~current_immerse_df[primary_identifier].isna(), None)
            else:
                current_immerse_df['correct_participant_id'] = None
        else:
            current_immerse_df['correct_participant_id'] = current_immerse_df.get(participant_identifier)

        # Case 1: Deletion IDs
        if self.delete_ids:
            if "movisens_esm" in system:
                # TODO: Verify functionality
                print("Debugging... deleting ids", self.delete_ids)
                current_immerse_df = current_immerse_df[~current_immerse_df.apply(
                    lambda row: (row.get(participant_identifier),
                                 row.get(participant_number),
                                 row.get('VisitCode'),
                                 row.get('SiteCode')) in self.delete_ids, axis=1)]
            elif 'movisens_fidelity' in system and primary_identifier:
                current_immerse_df = current_immerse_df[
                       ~current_immerse_df.apply(lambda row: _normalize_key(row.get(primary_identifier)) in self.delete_ids, axis=1)]
            else:
                current_immerse_df = current_immerse_df[~current_immerse_df.apply(
                    lambda row: _normalize_key(row.get(participant_identifier)) in self.delete_ids, axis=1)]

        # Case 2: Merging IDs
        if self.merge_ids:
            if "movisens_esm" in system:
                current_immerse_df['correct_participant_id'] = current_immerse_df.apply(
                    lambda row: self.merge_ids.get(row.get(participant_number), row.get('correct_participant_id')), axis=1)
            else:
                current_immerse_df['correct_participant_id'] = current_immerse_df.apply(
                    lambda row: self.merge_ids.get(_normalize_key(row.get(participant_identifier)), row.get('correct_participant_id')), axis=1)

        # Case 3: Adding IDS
        if self.add_ids:
            if "movisens_esm" in system:
                normalize_ids = {
                    tuple(_normalize_key(x) for x in k): value
                    for k, value in self.add_ids.items()
                }

                def lookup_row(row):
                    key = (
                        # _normalize_key(row.get(participant_identifier)),
                        _normalize_key(row.get(participant_number)),
                        row.get('Country'),
                        _normalize_key(row.get('VisitCode')),
                        # _normalize_key(row.get('SiteCode')),

                    )
                    return normalize_ids.get(key, row.get('correct_participant_id'))

                current_immerse_df['correct_participant_id'] = current_immerse_df.apply(lookup_row, axis=1)
            else:
                current_immerse_df['correct_participant_id'] = current_immerse_df.apply(
                    lambda row: self.add_ids.get(_normalize_key(row.get(participant_number)), row.get('correct_participant_id'))
                    if pd.isna(row.get('correct_participant_id')) or str(row.get('correct_participant_id')).strip() == ""
                    else row.get('correct_participant_id'), axis=1)

        # Case 4: Update IDS
        if self.update_ids:
            if self.debug:
                print("DEBUG: Current IDs to update : ", list(self.update_ids.items())[:10])

            if "movisens_esm" in system:
                normalize_ids = {
                    tuple(_normalize_key(x) for x in k): value
                    for k, value in self.update_ids.items()
                }

                def lookup_row(row):
                    key = (
                        _normalize_key(row.get(participant_identifier)),
                        _normalize_key(row.get(participant_number)),
                        _normalize_key(row.get('VisitCode')),
                        _normalize_key(row.get('SiteCode'))
                    )
                    return normalize_ids.get(key, row.get('correct_participant_id'))
                current_immerse_df['correct_participant_id'] = current_immerse_df.apply(lookup_row, axis=1)

            elif "maganamed" in system:
                # Apply corrected id (prefer structured dicts when present)
                def maganamed_lookup(row):
                    key = _normalize_key(row.get(participant_identifier))
                    val = self.update_ids.get(key)
                    if isinstance(val, dict):
                        return val.get('correct_participant_identifier') or row.get('correct_participant_id')
                    elif val is not None:
                        return val
                    # try corrected id key fallback
                    corr = _normalize_key(row.get('correct_participant_id'))
                    return self.update_ids.get(corr, row.get('correct_participant_id'))

                current_immerse_df['correct_participant_id'] = current_immerse_df.apply(maganamed_lookup, axis=1)

            elif "movisens_fidelity" in system and primary_identifier:
                current_immerse_df['correct_participant_id'] = current_immerse_df.apply(
                    lambda row: self.update_ids.get(_normalize_key(row.get(primary_identifier)),
                                                    row.get('correct_participant_id')), axis=1)
            else:
                current_immerse_df['correct_participant_id'] = current_immerse_df.apply(
                    lambda row: self.update_ids.get(_normalize_key(row.get(participant_identifier)),
                                                    row.get('correct_participant_id')), axis=1)

            # Extended update include cases where values for "unit, "condition" or "randomize" values are missing.
            def apply_extended_update_id(row):
                original_id = _normalize_key(row.get(participant_identifier))
                if original_id is None:
                    return row

                update_row = self.update_ids.get(original_id)
                # fallback to corrected id key if not found under original form
                if update_row is None:
                    corr = _normalize_key(row.get('correct_participant_id'))
                    update_row = self.update_ids.get(corr)

                if update_row is None:
                    return row

                # If update_row is dict (structured maganamed), use fields; otherwise treat as legacy string that only sets id
                if isinstance(update_row, dict):
                    if self.debug:
                        print('DEBUG: original_id', original_id, "update_id", update_row)
                    corrected = update_row.get('correct_participant_identifier')
                    if corrected is not None and str(corrected).strip() != '':
                        row['correct_participant_id'] = corrected

                    for col in ['unit', 'condition', 'randomize']:
                        value = row.get(col)
                        if not (pd.isna(value) or str(value).strip() == ''):
                            continue
                        update_value = update_row.get(col)
                        if update_value is None:
                            continue
                        norm_update = _normalize_numberish(update_value)
                        if norm_update is None:
                            continue
                        # guard: do not assign the participant id string into these columns
                        if str(norm_update).strip() == str(original_id).strip():
                            if self.debug:
                                print(f"DEBUG-WARNING: skipping assignment of participant id string into {col} for {original_id}")
                            continue
                        row[col] = norm_update
                else:
                    # legacy/plain case: only set corrected id
                    row['correct_participant_id'] = update_row
                return row

            if "maganamed" in system:
                current_immerse_df = current_immerse_df.apply(apply_extended_update_id, axis=1)

        # Case 5: Specific IDs according T-files
        if '_T0_' in filename and self.assign_id_to_T0:
            current_immerse_df[participant_identifier] = current_immerse_df.apply(
                lambda row: self.assign_id_to_T0.get((row.get(participant_identifier), row.get(participant_number)),
                                                     row.get(participant_identifier)), axis=1)

        if '_T1_' in filename and self.assign_id_to_T1:
            current_immerse_df[participant_identifier] = current_immerse_df.apply(
                lambda row: self.assign_id_to_T1.get((row.get(participant_identifier), row.get(participant_number)),
                                                     row.get(participant_identifier)), axis=1)

        if '_T2_' in filename and self.assign_id_to_T2:
            current_immerse_df[participant_identifier] = current_immerse_df.apply(
                lambda row: self.assign_id_to_T2.get((row.get(participant_identifier), row.get(participant_number)),
                                                     row.get(participant_identifier)), axis=1)

        if '_T3_' in filename and self.assign_id_to_T3:
            current_immerse_df[participant_identifier] = current_immerse_df.apply(
                lambda row: self.assign_id_to_T3.get((row.get(participant_identifier), row.get(participant_number)),
                                                     row.get(participant_identifier)), axis=1)

        if 'movisens_fidelity' in system and primary_identifier:
            target_col = primary_identifier
        else:
            target_col = participant_identifier

        current_immerse_df[target_col] = current_immerse_df.pop("correct_participant_id")
        return current_immerse_df

    # Complete ALL ids which 'unit', 'condition', 'randomize' are missing.
    def add_unit_site_and_randomized_values(self, cleand_df, id_column):
        reference_df = get_columns_from_id_reference()
        if 'correct_participant_identifier' not in reference_df.columns:
            raise KeyError("Reference table must contain 'correct_participant_identifier'")

        # Normalize reference index and drop duplicates
        reference_df['correct_participant_identifier'] = reference_df['correct_participant_identifier'].astype(str).str.strip()
        if reference_df['correct_participant_identifier'].duplicated(keep=False).any():
            if self.debug:
                print("DEBUG: duplicates in id reference; keeping first occurrence for lookup")
            reference_df = reference_df.drop_duplicates(subset=['correct_participant_identifier'], keep='first')

        ref_lookup = reference_df.set_index('correct_participant_identifier')

        def update_row(row):
            original_id_raw = row.get(id_column, '')
            original_id = '' if pd.isna(original_id_raw) else str(original_id_raw).strip()
            if original_id == '':
                return row
            if original_id not in ref_lookup.index:
                return row
            update_info = ref_lookup.loc[original_id]
            for col in ['unit', 'condition', 'randomize']:
                if col not in update_info.index:
                    continue
                current_val = row.get(col, None)
                missing_in_row = pd.isna(current_val) or str(current_val).strip() == ''
                if not missing_in_row:
                    continue
                ref_val = update_info.get(col)
                norm_ref_val = _normalize_numberish(ref_val)
                if norm_ref_val is None:
                    continue
                # Guard: do not set a participant id string into unit/condition/randomize
                if str(norm_ref_val).strip() == original_id:
                    if self.debug:
                        print(f"DEBUG-WARNING: reference value equals id for {original_id} col {col}; skipping")
                    continue
                row[col] = norm_ref_val
            return row

        updated_df = cleand_df.apply(update_row, axis=1)
        return updated_df

    # ---> Step 3.  Changes to apply to ORIGINAL_IMMERSE_SOURCE
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
                print(f"\nApplying changes to {filename}...")
                df = dataframe.copy()
                try:
                    if 'movisens_fidelity' in immerse_system:
                        for col_id in config['column_id']:
                            print(f"Processing column {col_id},...")
                            df = self._apply_changes_from_rulebook(
                                df,
                                col_id,
                                config['column_id_number'],
                                filename,
                                immerse_system
                            )

                        existing_columns = [col for col in rename_fidelity_columns_dict if col in df.columns]
                        df.rename(columns={col: rename_fidelity_columns_dict[col] for col in existing_columns}, inplace=True)

                    else:
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
                    output_path = cleaned_folder / filename.replace("adjusted", "cleaned")
                    df.to_csv(output_path, sep=";", index=False)

                    if 'movisens_fidelity' in immerse_system:
                        df_ids_filtered = df[['participant_number', 'fidelity_participant_identifier_T1', 'time',
                                              'reported_patient_identifier', 'rater_participant_identifier',
                                              'fidelity_participant_identifier_T2', 'item_770']]
                        new_output_path = cleaned_folder / f'extra_{filename.replace("adjusted", "ids_filtered")}'
                        df_ids_filtered.to_csv(new_output_path, sep=";", index=False)

                    print(f"Exported cleaned file to {output_path}")
                    immerse_clean_dfs[filename] = df

                except Exception as e:
                    print(f"Execute corrections function. Unexpected error in {filename}: {repr(e)}")
                    traceback.print_exc()

            return immerse_clean_dfs

    # Identified changes from Issues & Rulebook.
    def issues_to_correct_from_rulebook(self, rulebook, fixes_path, filename):
        '''
        Detected issues are merged with the rulebook to create a merged filed with the specific changes
        that should be carried out.
        '''

        df_issues = self.df.copy()
        merged_ids_rulebook_df = rulebook

        merged_ids_rulebook_df.rename(
            columns={merged_ids_rulebook_df.columns[0]: 'participant_identifier'}, inplace=True)

        # Movisens ESM has more than ONE 'key' to identify a participant ID.
        if "movisens_esm" in filename:
            merged_ids_rulebook_df.rename(
                columns={merged_ids_rulebook_df.columns[1]: 'participant_number'}, inplace=True)
            merged_ids_rulebook_df.rename(
                columns={merged_ids_rulebook_df.columns[5]: 'correct_participant_identifier'},
                inplace=True)

            # Change float to int values
            if 'VisitCode' in merged_ids_rulebook_df.columns:
                merged_ids_rulebook_df['VisitCode'] = merged_ids_rulebook_df['VisitCode'].apply(
                    lambda x: int(x) if isinstance(x, float) and not pd.isnull(x) else x)

            # # Change float to int values
            if 'SiteCode' in merged_ids_rulebook_df.columns:
                merged_ids_rulebook_df['SiteCode'] = pd.to_numeric(merged_ids_rulebook_df['SiteCode'],
                                                                   errors='coerce').astype('Int64')

        # Option 1: Use a copy of the rulebook with the new column naming convention.
        updated_rulebook_df = merged_ids_rulebook_df.copy()

        # Option 2: Merging issues with rulebook to define and export changes.csv
        df_issues['participant_identifier'] = df_issues['participant_identifier']
        merged_ids_rulebook_df['participant_identifier'] = merged_ids_rulebook_df[
            'participant_identifier'].astype(str)

        self.changes_df = pd.merge(df_issues, merged_ids_rulebook_df, on='participant_identifier', how='inner')
        self.changes_df.to_csv(os.path.join(fixes_path, f'identified_id_issues_and_changes_extracted_ids_{filename}'), index=False)
        updated_rulebook_df.to_csv(os.path.join(fixes_path, f'updated_rulebook.csv'), index=False)
        return updated_rulebook_df

    # ---> Step 1: Identify and prepare IDs which must be corrected.
    def prepare_ids_correction(self, rulebook, changes_path, filename):
        '''
          The following method requires 2 input sources to clean ids:
          - rulebook: Contains all participant ids and their corrected id per system.
          - changes_path: Contains only those ids which were identified with any issue in original dataset.
        '''

        print(f"\n\033[32mStarting cleaning process from '{filename}' \033[0m\n")
        merged_issues_with_rulebook = self.issues_to_correct_from_rulebook(rulebook, changes_path, filename)
        return merged_issues_with_rulebook
