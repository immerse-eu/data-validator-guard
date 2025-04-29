VALID_SITE_CODES_AND_CENTER_NAMES = {
        1: 'Lothian',
        2: 'Camhs',
        3: 'Mannheim',
        4: 'Wiesloch',
        5: 'Leuven',
        6: 'Bierbeek',
        7: 'Bratislava',
        8: 'Kosice'
    }

class MaganamedValidation:

    def __init__(self, df):
        self.magana_df = df
        self.magana_issues = {}


    def validate_site_and_center_name_id(self, site_column, center_name_column, study_id_column):
        issues = []

        self.magana_df[center_name_column] = self.magana_df[center_name_column].str.strip().str.upper()
        self.magana_df[study_id_column] = self.magana_df[study_id_column].str.strip().str.upper()
        self.magana_df['abbreviation_center_name'] = self.magana_df[center_name_column].str[0:2]

        # Validation of participant_ID & Center Name
        self.magana_df['validation_IDs_result'] = self.magana_df.apply(lambda row: 'OK' if row['abbreviation_center_name'] in row[study_id_column] else 'ID-mismatch', axis=1)
        results = self.magana_df[['abbreviation_center_name', study_id_column, 'validation_IDs_result']]
        filter_id_issues = results[results.validation_IDs_result == 'ID-mismatch']

        if not filter_id_issues.empty:
            print(f"❌ | Issues found in IDS:\n'{filter_id_issues}")
            issues.append(filter_id_issues)
        else:
            print(" ✔ | Validation of IDS passed: No typos found! }")

        # Validation of Site
        self.magana_df[site_column] = self.magana_df[site_column]
        normalised_control_dict = {k: v.upper() for k, v in VALID_SITE_CODES_AND_CENTER_NAMES.items()}
        self.magana_df['site_results'] = self.magana_df.apply(
            lambda row: 'OK' if normalised_control_dict.get(row[site_column]) == row[center_name_column] else 'Site-mismatch',axis=1)
        site_issues = self.magana_df[[site_column, center_name_column, 'site_results']]
        filter_site_issues = site_issues[site_issues['site_results'] == 'Site-mismatch']

        if not site_issues.empty:
            print(f"❌ | Issues found in Site column :\n'{filter_site_issues}")
            issues.append(filter_site_issues)
        else:
            print(" ✔ | Validation of IDS passed: No typos found! }")