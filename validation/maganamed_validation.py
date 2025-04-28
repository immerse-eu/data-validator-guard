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
        self.managa_df = df
        self.managa_issues = {}


    def validate_site_and_center_name_id(self, control, site_column, center_name_column, study_id_column):
        issues = []

        self.managa_df[center_name_column] = self.managa_df[center_name_column].str.strip().str.upper().str[0:2]
        self.managa_df[study_id_column] = self.managa_df[study_id_column].str.strip().str.upper()

        self.managa_df['validation_result'] = self.managa_df.apply(lambda row: 'OK' if row[center_name_column] in row[study_id_column] else 'ID-mismatch', axis=1)
        results = self.managa_df[[center_name_column, study_id_column,'validation_result']]
        filter_issues = results[results.validation_result == 'ID-mismatch']
        issues.extend(filter_issues)

        print("\nIssues: \n", filter_issues)








