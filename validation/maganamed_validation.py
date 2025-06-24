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
    with open("./config/config.yaml", "r", encoding="utf-8") as path:
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
    print(f"\n Successfully '{excel_filename}' exported.")


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

    def validate_completion_questionaries(self, table_name):
        column_questionaries = [column for column in self.magana_df.columns if column.startswith('SAQ')
                                and not column.startswith('SAQ_total')]

        def is_valid_response(x):
            return not (pd.isna(x) or str(x).strip() == '')

        self.magana_df['count_responses'] = self.magana_df[column_questionaries].apply(
            lambda row: row.apply(is_valid_response).sum(), axis=1)

        self.magana_df['percentage_qre_completed'] = (
                self.magana_df['count_responses'] / len(column_questionaries) * 100).round(1).astype(int)

        filter_by_80_percent = self.magana_df[self.magana_df['percentage_qre_completed'] >= 80]

        print(f" Number of question columns: {len(column_questionaries)}")
        print(f" Responses with ≥80% completion: {len(filter_by_80_percent)}")
        print(self.magana_df[['participant_identifier', 'visit_name', 'count_responses', 'percentage_qre_completed']])

        # export_table(self.magana_df, table_name)
        return self.magana_df

    #  TODO: Clean and export verified Dx to new_db.
    def validate_primary_diagnosis(self, table_name):
        self.magana_df['visit_name'] = self.magana_df['visit_name'].str.strip()

        filtering_baseline_and_screening = self.magana_df[
            self.magana_df['visit_name'].isin(['Baseline (clinician)', 'Screening'])]
        column_loinc_codes = [column for column in self.magana_df if column.startswith('F')]

        coincidences = []
        for index, row in filtering_baseline_and_screening.iterrows():

            coincidence_columns = []
            for column_code in column_loinc_codes:
                result_match_validation = f'{column_code}_matches_primary_Dx'
                if result_match_validation not in filtering_baseline_and_screening.columns:
                    filtering_baseline_and_screening[result_match_validation] = (
                        filtering_baseline_and_screening.groupby('participant_identifier')[
                            column_code].transform(lambda x: (x == x.iloc[0]).map({True: 'yes', False: 'no'})))
                if filtering_baseline_and_screening.loc[index, result_match_validation] == 'yes':
                    coincidence_columns.append(column_code)
            if coincidence_columns:
                coincidences.append(f'coincidences in {" ".join(coincidence_columns)}')
            else:
                coincidences.append('no coincidences')

        filtering_baseline_and_screening.loc[:, 'coincidences'] = coincidences
        # filtering_baseline_and_screening['coincidences'] = coincidences

        # Issues:
        filtering_baseline_and_screening_issues = filtering_baseline_and_screening[
            filtering_baseline_and_screening['coincidences'] == "no coincidences"]

        export_table(filtering_baseline_and_screening_issues, table_name)
        export_table(filtering_baseline_and_screening, table_name)

    def retrieve_saq_data(self):
        self.validate_completion_questionaries('Service-Attachement-Questionnaire-(SAQ)')
        self.magana_df['visit_name'] = self.magana_df['visit_name'].str.strip().str.extract(r'^(\w+)', expand=False)
        return self.magana_df[['participant_identifier', 'visit_name', 'count_responses', 'percentage_qre_completed']]

    # TODO: Enhance outcome: Example,once finding a match compare idf the other periods have empty responses, if not, is an issue.
    def validate_completed_visits(self, auxiliar_magana_df):
        self.magana_df['end_01'] = self.magana_df[
                                       'end_01'] - 1  # Normalized column with "new" VALID_TYPE_VISIT_ATTENDANCE

        # Comparison "VALID_TYPES_DICT" between SAQ and END tables, "visit_name" & "end_01" columns.
        merged_magana_df = self.magana_df.merge(auxiliar_magana_df, on='participant_identifier', how='left')
        merged_magana_df['does_end_01_matches_with_saq_visit'] = merged_magana_df.apply(
            lambda row: 'Yes' if VALID_TYPE_VISIT_ATTENDANCE.get(row['end_01']) == row['visit_name_y']
            else 'No', axis=1)

        merged_magana_df = merged_magana_df.rename(
            columns={'visit_name_x': 'visit_name', 'visit_name_y': 'clean_visit_name'})
        merged_magana_df = merged_magana_df.drop(columns=['count_responses'])
        export_table(merged_magana_df, table_name='END_SAQ')

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

        # TODO: Update "is_a_valid_period" to True/false. Current implementation 1/0.
        self.magana_df['is_a_valid_period'] = abs(
            self.magana_df['estimated_duration_study_in_days'] - self.magana_df['duration_study_in_days']) <= 10

        print(self.magana_df[['duration_study_in_days', 'estimated_duration_study_in_days', 'is_a_valid_period']].head(
            10))
        export_table(self.magana_df, f'{table_name}')

    def passed_validation(self, table_name):
        if len(self.magana_issues) == 0:
            print("\n ✔  | All validations were successfully passed!!")
            return True
        else:
            return self.export_managamed_issues(table_name)
