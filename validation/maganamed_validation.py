import os

import pandas as pd
import yaml
from config.config_loader import load_config_file

VALID_SITE_CODES_AND_CENTER_NAMES = {
    1: 'Lothian',   # L0
    2: 'Camhs',     # L0
    3: 'Mannheim',  # L1
    4: 'Wiesloch',  # L1
    5: 'Leuven',    # L2
    6: 'Bierbeek',  # L2
    7: 'Bratislava',# L3
    8: 'Kosice'     # L3
}

VALID_LANGUAGE_SELECTION = {
    0: 'EN',
    1: 'GE',
    2: 'BE',
    3: 'SK'
}

VALID_PARTICIPANTS_TYPE = {
    0: 'Patient',
    1: 'Clinician',
    2: ['Teamlead', 'Admin'],  # Teamlead / Admin
    3: ['Finance ', 'accounting staff']  # Finance / accounting staff
}

VALID_TYPE_VISIT_ATTENDANCE = {
    -1: 'Enrolment',
    0: 'Baseline',
    1: 'T1',
    2: 'T2',
    3: 'T3',
}

VALID_STUDY_PERIOD_IN_MONTHS = {
    'Baseline': 0,
    'T1': 2,
    'T2': 6,
    'T3': 12,
}

new_key_center_name = list(VALID_SITE_CODES_AND_CENTER_NAMES.values())
new_value_language_code = list(VALID_LANGUAGE_SELECTION.keys())
VALID_CENTER_AND_LANGUAGE = {k: new_value_language_code[i // 2] for i, k in enumerate(new_key_center_name)}

new_key_center_code = list(VALID_SITE_CODES_AND_CENTER_NAMES.keys())
new_value_language_abbrev = list(VALID_LANGUAGE_SELECTION.values())
VALID_CENTER_AND_ACRONYM = {k: new_value_language_abbrev[i // 2] for i, k in enumerate(new_key_center_code)}

output_excel_path = load_config_file('reports', 'issues')
output_csv_path = "validation_issues.csv"  # TODO: homologate to common "issues" folder.


def import_custom_csr_df_with_language_selection():
    with open("./config/copy_config.yaml", "r", encoding="utf-8") as path:  # TODO: change according to config yaml file
        config = yaml.safe_load(path)
    csri = config['auxiliarFiles']['csri']

    df = pd.read_csv(csri)
    df = df.drop_duplicates()
    filtered_df = df[df['center_name'].notna()]
    path = os.path.dirname(csri)
    filtered_df.to_csv(os.path.join(path, "filter_crsi_file.csv"), index=False)
    print(f"\n Length auxiliar-language-csri-file: {filtered_df.shape[0]} rows")
    return filtered_df


def export_table(df_to_export, table_name):
    excel_filename = f'{table_name}.xlsx'
    filepath = os.path.join(output_excel_path, excel_filename)
    df_to_export.to_excel(filepath, index=False)
    df_to_export.to_csv(filepath.replace(".xlsx", ".csv"), sep=';', index=False)
    print(f"\n Successfully '{excel_filename}' exported {output_excel_path}.")


class MaganamedValidation:

    def __init__(self, df):
        self.magana_df = df
        self.magana_issues = []

    def export_managamed_issues(self, table_name):

        excel_filename = f'{table_name}_issues.xlsx'
        filepath = os.path.join(output_excel_path, excel_filename)
        export_issues = pd.concat(self.magana_issues, ignore_index=True)
        export_issues = export_issues.groupby(["participant_identifier", "SiteCode", "center_name"],
                                              as_index=False).first()

        export_issues.to_excel(filepath, index=False)
        export_issues.to_csv(filepath.replace(".xlsx", ".csv"), sep=';', index=False)

        print(f"\n All issues found in '{table_name}', have been as '{excel_filename}' exported.")

    def validate_site_and_center_name_id(self, site_column, center_name_column, study_id_column):

        # Normalization process
        self.magana_df[center_name_column] = self.magana_df[center_name_column].str.strip().str.upper()
        self.magana_df[study_id_column] = self.magana_df[study_id_column].str.strip()
        self.magana_df['abbreviation_center_name'] = self.magana_df[center_name_column].str[0:2]

        # Validation of participant_ID & Center Name
        self.magana_df['id_validation_result'] = self.magana_df.apply(
            lambda row: 'OK' if row['abbreviation_center_name'] in row[study_id_column] else 'ID-mismatch', axis=1)
        results = self.magana_df[
            [study_id_column, site_column, center_name_column, 'abbreviation_center_name', 'id_validation_result']]
        filter_id_issues = results[results.id_validation_result == 'ID-mismatch']

        if not filter_id_issues.empty:
            print(f"\n❌ {len(filter_id_issues)} | Issues have been found in participants IDs.")
            self.magana_issues.append(filter_id_issues)
        else:
            print("\n ✔ | Validation of IDS passed: No issues were detected in participant IDs! ")

        # Validation of Site
        self.magana_df[site_column] = self.magana_df[site_column]
        normalised_control_dict = {k: v.upper() for k, v in VALID_SITE_CODES_AND_CENTER_NAMES.items()}
        self.magana_df['site_validation_result'] = self.magana_df.apply(
            lambda row: 'OK' if normalised_control_dict.get(row[site_column]) == row[
                center_name_column] else 'Site-mismatch', axis=1)
        site_issues = self.magana_df[
            [study_id_column, site_column, center_name_column, 'abbreviation_center_name', 'site_validation_result']]
        filter_site_issues = site_issues[site_issues['site_validation_result'] == 'Site-mismatch']

        if not filter_site_issues.empty:
            print(f"\n❌ {len(filter_site_issues)} | Issues have been found in 'Site' column.")
            self.magana_issues.append(filter_site_issues)
        else:
            print("\n ✔ | Validation of 'Site' passed: No issues were detected in 'Site' columns!")

    def validate_special_duplication_types(self, column):
        issues = []

        normalised_column = self.magana_df[column].str.strip()
        filter_normalised_column_with_additional_characters = normalised_column[
            normalised_column.str.contains(r'[_-]?v', case=False, regex=True)]

        self.magana_df['normalised_column'] = self.magana_df[column].str.replace(r'[_-]?v$', '', case=False, regex=True)
        self.magana_df['is_duplicate'] = self.magana_df['normalised_column'].duplicated(keep=False)

        filter_issues = self.magana_df[self.magana_df['is_duplicate'] == True]
        if filter_issues.empty:
            print(f"\n ✔ | Validation of special duplications passed: No duplications were found in column '{column}'.")
        else:
            print(f"\n❌ | {len({filter_issues})} Issues found in '{column}' column ")
            # print(f"\n❌ | Issues found in '{column}' column :\n'{filter_issues}")
            issues.append(filter_issues)
            return issues

        print(f"\nAdditional observations from '{column}': \n", filter_normalised_column_with_additional_characters)

    def validate_auxiliar_table(self, study_id_column, center_name_column):
        # Normalization
        self.magana_df[study_id_column] = self.magana_df[study_id_column].str.strip()
        self.magana_df[center_name_column] = self.magana_df[center_name_column].str.strip().str.capitalize()
        # Validation
        self.magana_df['language_validation_result'] = self.magana_df.apply(
            lambda row: 'OK' if VALID_CENTER_AND_LANGUAGE.get(row[center_name_column]) == row[
                'PARTICIPANT_02'] else 'language-mismatch', axis=1)
        # Filtering
        filter_participant_language_val = self.magana_df[[study_id_column, 'language_validation_result']]
        filter_issues = self.magana_df[filter_participant_language_val['language_validation_result'] != 'OK']

        if filter_issues.empty:
            print(f"\n ✔ | Language validation from 'all_csri_with_languages', successfully passed")
            return filter_participant_language_val
        else:
            print(f"\n❌ | Issues found in '{self}' :\n'{filter_issues}")
            self.magana_issues.append(filter_issues)

    def validate_language_selection(self, table_name, site_column):
        # Validation 2: Table_name and SiteCode
        self.magana_df['language_validation_result'] = self.magana_df.apply(
            lambda row: 'OK' if VALID_CENTER_AND_ACRONYM.get(
                row[site_column]) == table_name else 'language-mismatch', axis=1)

        # Filtering
        filter_participant_language_val = self.magana_df[[site_column, 'language_validation_result']]
        filter_issues = self.magana_df[filter_participant_language_val['language_validation_result'] != 'OK']

        if filter_issues.empty:
            print(f"\n ✔ | Language validation from '{table_name}', successfully passed")
            return filter_participant_language_val
        else:
            print(f"\n❌ | Issues found in '{self}' :\n'{filter_issues}")
            self.magana_issues.append(filter_issues)

    def validate_completion_questionnaires(self, table_name):
        column_questionnaires = [column for column in self.magana_df.columns if column.startswith('SAQ')
                                and not column.startswith('SAQ_total')]

        def is_valid_response(x):
            return not (pd.isna(x) or str(x).strip() == '')

        self.magana_df['count_responses'] = self.magana_df[column_questionnaires].apply(
            lambda row: row.apply(is_valid_response).sum(), axis=1)

        self.magana_df['percentage_qre_completed'] = (
                self.magana_df['count_responses'] / len(column_questionnaires) * 100).round(1).astype(int)

        filter_by_80_percent = self.magana_df[self.magana_df['percentage_qre_completed'] >= 80]

        print(f" Number of question columns: {len(column_questionnaires)}")
        print(f" Responses with ≥80% completion: {len(filter_by_80_percent)}")

        # Export csv/xlsx files (optional)
        # export_table(self.magana_df, table_name)
        return self.magana_df

    def validate_primary_diagnosis(self, table_name, export=True):
        self.magana_df['visit_name'] = self.magana_df['visit_name'].str.strip()

        mask_baseline_screening = self.magana_df['visit_name'].isin(['Baseline (clinician)', 'Screening'])
        baseline_idx = self.magana_df.loc[mask_baseline_screening].index
        column_loinc_codes = [col for col in self.magana_df.columns if str(col).upper().startswith('F')]

        for column_code in column_loinc_codes:
            result_match_validation = f'{column_code}_matches_primary_Dx'
            if result_match_validation not in self.magana_df.columns:
                self.magana_df[result_match_validation] = pd.NA

        # helper: get family group from a code string like 'F33' or 'F20-29' -> returns 'F30' or 'F20'
        def _family_group_from_code(code_str):
            if not isinstance(code_str, str):
                return pd.NA
            m = pd.Series([code_str.upper()]).str.extract(r'(F)(\d+)', expand=True)
            if m.isna().any().any():
                return pd.NA
            try:
                num = int(m.iloc[0, 1])
            except Exception:
                return pd.NA
            group = (num // 10) * 10
            return f'F{group}'

        if len(baseline_idx) > 0 and 'diagn_primary' in self.magana_df.columns and column_loinc_codes:
            diagn_series = self.magana_df.loc[baseline_idx, 'diagn_primary'].astype(str).str.upper()
            diagn_first = diagn_series.str.extract(r'(F\d+)', expand=False)
            diagn_group = diagn_first.apply(lambda x: _family_group_from_code(x) if pd.notna(x) else pd.NA)

            # For each F-column compute matches only for baseline rows
            for column_code in column_loinc_codes:
                result_match_validation = f'{column_code}_matches_primary_Dx'

                col_family_raw = str(column_code).upper()
                col_family_group = _family_group_from_code(col_family_raw)
                if pd.isna(col_family_group):
                    continue

                # presence indicator in the column for baseline rows: non-empty, non-zero, not NaN
                col_values = self.magana_df.loc[baseline_idx, column_code]
                presence = ~(
                        col_values.isna() |
                        col_values.astype(str).str.strip().eq('') |
                        col_values.astype(str).str.strip().eq('0') |
                        col_values.astype(str).str.strip().str.lower().eq('nan')
                )

                # build matches for baseline rows
                matches = []
                for idx_i, dfam in diagn_group.items():
                    if pd.isna(dfam):
                        matches.append(pd.NA)
                    else:
                        if presence.loc[idx_i] and dfam == col_family_group:
                            matches.append('yes')
                        else:
                            matches.append('no')

                self.magana_df.loc[baseline_idx, result_match_validation] = pd.Series(matches,
                                                                                      index=baseline_idx).values

        # Build the 'coincidences' column for baseline/screening rows only
        def _build_coincidence_string(cols):
            if not cols:
                return 'no coincidences'
            return 'coincidences with ' + ','.join(cols)

        coincidences = []
        for idx in baseline_idx:
            matched_cols = []
            for col in column_loinc_codes:
                val = self.magana_df.at[idx, f'{col}_matches_primary_Dx']
                if pd.notna(val) and val == 'yes':
                    matched_cols.append(col)
            coincidences.append(_build_coincidence_string(matched_cols))

        if 'coincidences' not in self.magana_df.columns:
            self.magana_df['coincidences'] = pd.NA
        self.magana_df.loc[baseline_idx, 'coincidences'] = coincidences

        # Prepare issues subset: baseline rows with 'no coincidences'
        filtering_baseline_and_screening = self.magana_df.loc[mask_baseline_screening].copy()
        filtering_baseline_and_screening_issues = filtering_baseline_and_screening[
            filtering_baseline_and_screening['coincidences'] == "no coincidences"
            ]

        # Optional exports:
        if export:
            export_table(self.magana_df, f"{table_name}_primary_diagnosis_full")
            export_table(filtering_baseline_and_screening, f"{table_name}_primary_diagnosis_baseline_screening")
            export_table(filtering_baseline_and_screening_issues, f"{table_name}_primary_diagnosis_issues")

        # Store issues
        if not filtering_baseline_and_screening_issues.empty:
            self.magana_issues.append(filtering_baseline_and_screening_issues)

        return self.magana_df

    def retrieve_saq_data(self):
        self.validate_completion_questionnaires('Service-Attachement-Questionnaire-(SAQ)')
        self.magana_df['visit_name'] = self.magana_df['visit_name'].str.strip().str.extract(r'^(\w+)', expand=False)
        return self.magana_df[['participant_identifier', 'visit_name', 'count_responses', 'percentage_qre_completed']]

    def validate_completed_visits(self, auxiliar_magana_df):
        # Normalized column with "new" VALID_TYPE_VISIT_ATTENDANCE
        self.magana_df['end_01'] = self.magana_df['end_01'] - 1

        # Comparison "VALID_TYPES_DICT" between SAQ and END tables, "visit_name" & "end_01" columns.
        merged_magana_df = self.magana_df.merge(auxiliar_magana_df, on='participant_identifier', how='left')
        merged_magana_df['does_end_01_matches_with_saq_visit'] = merged_magana_df.apply(
            lambda row: 'Yes' if VALID_TYPE_VISIT_ATTENDANCE.get(row['end_01']) == row['visit_name_y']
            else 'No', axis=1)

        merged_magana_df = merged_magana_df.rename(
            columns={'visit_name_x': 'visit_name', 'visit_name_y': 'clean_visit_name'})
        merged_magana_df = merged_magana_df.drop(columns=['count_responses'])
        export_table(merged_magana_df, table_name='END_SAQ')
        return merged_magana_df

    def validate_periods(self, table_name):
        self.magana_df['clean_visit_name'] = self.magana_df['visit_name'].str.strip().str.extract(r'^(\w+)',
                                                                                                  expand=False)

        for column in self.magana_df[['created_at', 'started_at', 'finished_at']]:
            self.magana_df[column] = pd.to_datetime(self.magana_df[column]).dt.date

        baseline_participants = {}
        for index, row in self.magana_df.iterrows():
            if 'Baseline' in row['visit_name']:
                baseline_participants[row['participant_identifier']] = row['started_at']

        def calculate_delta_time(row):
            baseline = baseline_participants.get(row['participant_identifier'])
            if pd.isna(baseline) or pd.isna(row['finished_at']):
                return pd.NaT
            return row['finished_at'] - baseline

        self.magana_df['duration_study_in_days'] = self.magana_df.apply(calculate_delta_time, axis=1).astype(
            str).str.extract(r'(\d+)').astype(float)

        self.magana_df['estimated_duration_study_in_days'] = self.magana_df['clean_visit_name'].map(
            lambda x: VALID_STUDY_PERIOD_IN_MONTHS.get(x, 0) * 30)

        self.magana_df['is_a_valid_period'] = (abs(
            self.magana_df['estimated_duration_study_in_days'] - self.magana_df['duration_study_in_days']) <= 10).map(
            {True: 'Yes', False: 'No'})

        # export_table(self.magana_df, f'{table_name}')
        return self.magana_df

    def passed_validation(self, table_name):
        if len(self.magana_issues) == 0:
            print("\n ✔  | All validations were successfully passed!!")
            return True
        else:
            return self.export_managamed_issues(table_name)
