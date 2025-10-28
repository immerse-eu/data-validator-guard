import re
import pandas as pd

VALID_TYPE_VISIT_ATTENDANCE = {
    0: 'T0',  # Baseline
    1: 'T1',
    2: 'T2',
    3: 'T3',
}

SITE_CODING_ESM_BY_LAND = {
     "UK": {1: 'LO', 2: 'CA'},
     "GE": {3: 'MA', 4: 'WI'},
     "BE": {5: 'LE', 6: 'BI'},
     "SK": {7: 'BR'},
     "SK_Female": {7: 'BR'},
     "SK_Kosice": {8: 'KO'},
     "SK_Kosice_Female": {8: 'KO'}
}


class MovisensxsValidation:

    def __init__(self, df):
        self.movisensxs_df = df.copy()
        self.movisensxs_issues = []

    def get_expected_visitcode(self, filename):
        '''
        VisitCode is related to T0-T3 values. This info is taken from a filename and compared with the valid dict; when
        a conditions matches, then it returns a key value.
        '''

        visit_match = re.search(r'_(T\d)_', filename)
        if visit_match is not None:
            extracted_visit_value = visit_match.group(1)
            for key, value in VALID_TYPE_VISIT_ATTENDANCE.items():
                if value == extracted_visit_value:
                    return key
            return None

    def extract_site_from_id(self, id_str):
        match = re.match(r"I[-_](\w+)[-_]P[-_]?\d+\w*", id_str)
        if match is not None:
            return match.group(1) if match else None

    def get_expected_sitecode(self, site_str):
        for site_dict in SITE_CODING_ESM_BY_LAND.values():
            for key, value in site_dict.items():
                if value == site_str:
                    return key
        return None

    def validate_visit_and_site_assignation(self, filename):
        expected_visit_code = self.get_expected_visitcode(filename)

        for _, row in self.movisensxs_df.iterrows():
            issues = {"participant_identifier": row.iloc[0]}
            extracted_site_value = self.extract_site_from_id(row.iloc[0])
            expected_site_code = self.get_expected_sitecode(extracted_site_value)

            # --- VisitCode ---
            visit_code = row['VisitCode']
            if pd.isna(visit_code) or expected_visit_code != visit_code:
                issues['VisitCode_expected'] = expected_visit_code
                issues['VisitCode_actual'] = visit_code

            # --- SiteCode ---
            site_code = row['SiteCode']
            if pd.isna(site_code) or expected_site_code != site_code:
                # print("pid", row["participant_identifier"], "extracted_site_value: ", extracted_site_value,
                # "expected_site_code: ", expected_site_code, "siteCode: ", site_code)
                issues['SiteCode_expected'] = expected_site_code
                issues['SiteCode_actual'] = site_code

        if len(issues) > 1:
            issues['filename'] = filename
            self.movisensxs_issues.append(issues)

    def generate_issues_report(self, filename):
        if not self.movisensxs_issues:
            print("✔ All validations passed. No issues found!")
        else:
            print(f"❌ Issues found. Report saved")
            df_report = pd.DataFrame(self.movisensxs_issues)
            print(df_report)
            return df_report.to_csv(f"{filename}_issues.csv", sep=";", index=False)
            # return self.movisensxs_issues
