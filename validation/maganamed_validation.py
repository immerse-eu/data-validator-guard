import pandas as pd
import yaml

from validation.general_validation import DataValidator

VALID_SITE_CODES_AND_CENTER_NAMES = {
    1: 'Lothian',   # L0
    2: 'Camhs',     # L
    3: 'Mannheim',  # L1
    4: 'Wiesloch',  # L1
    5: 'Leuven',    # L2
    6: 'Bierbeek',  # L2
    7: 'Bratislava',# L3
    8: 'Kosice'     # L3
}

VALID_LANGUAGE_SELECTION = {
    0: 'English',
    1: 'German',
    2: 'Belgian',
    3: 'Slovak'
    }

VALID_PARTICIPANTS_TYPE = {
    0: 'Patient',
    1: 'Clinician',
    2: ['Teamlead', 'Admin'], # Teamlead / Admin
    3: ['Finance ', 'accounting staff'] # Finance / accounting staff
    }



output_csv_path = "validation_issues.csv"

def import_custom_csr_df_with_language_selection():

    with open("./config/config.yaml", "r", encoding="utf-8") as path:
        config = yaml.safe_load(path)
    csri = config['auxiliarFiles']['csri']

    df = pd.read_csv(csri)
    new_df = df.drop_duplicates()
    new_df.to_csv("filter_crsi_file.csv", index=False)
    return new_df


class MaganamedValidation:

    def __init__(self, df):
        self.magana_df = df
        self.magana_issues = []

    def validate_site_and_center_name_id(self, site_column, center_name_column, study_id_column):

        # Normalization process
        self.magana_df[center_name_column] = self.magana_df[center_name_column].str.strip().str.upper()
        self.magana_df[study_id_column] = self.magana_df[study_id_column].str.strip().str.upper()
        self.magana_df['abbreviation_center_name'] = self.magana_df[center_name_column].str[0:2]

        # Validation of participant_ID & Center Name
        self.magana_df['id_validation_result'] = self.magana_df.apply(
            lambda row: 'OK' if row['abbreviation_center_name'] in row[study_id_column] else 'ID-mismatch', axis=1)
        results = self.magana_df[
            [study_id_column, site_column, center_name_column, 'abbreviation_center_name', 'id_validation_result']]
        filter_id_issues = results[results.id_validation_result == 'ID-mismatch']

        if not filter_id_issues.empty:
            print(f"\n❌ {len(filter_id_issues)} | Issues have been found in participants IDs:\n {filter_id_issues}")
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
            print(f"\n❌ {len(filter_site_issues)} | Issues have been found in 'Site' column :\n{filter_site_issues}")
            self.magana_issues.append(filter_site_issues)
        else:
            print("\n ✔ | Validation of 'Site' passed: No issues were detected in 'Site' columns!")

        if self.magana_issues:
            all_issues_df = pd.concat(self.magana_issues, ignore_index=True)
            all_issues_df = all_issues_df.groupby([study_id_column, site_column, center_name_column],
                                                  as_index=False).first()
            all_issues_df.to_csv(output_csv_path, sep=';', index=False)
            print(f"\n Issues exported to '{output_csv_path}' file.")
        else:
            print("\n ✔  | All validations were successfully passed!!")

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
            print(f"\n❌ | Issues found in '{column}' column :\n'{filter_issues}")
            issues.append(filter_issues)
            return issues

        print(f"\nAdditional observations from '{column}': \n", filter_normalised_column_with_additional_characters)

    def validate_language_selection(self, column):
        self.magana_df[column] = self.magana_df[column].str.strip().str.upper()

    def passed_validation(self):
        return len(self.magana_issues) == 0