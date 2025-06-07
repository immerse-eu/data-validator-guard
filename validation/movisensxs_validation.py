VALID_TYPE_VISIT_ATTENDANCE = {
    0: 'T0',  # Baseline
    1: 'T1',
    2: 'T2',
    3: 'T3',
}

''' "SITE_CODE_MAPPING" is unclear how was defined by GH,  but I am going to use suggested codes from Anita:'''

SUGGESTION_SITE_CODING_ESM = {
    0: "UK",
    1: "GE",
    2: "BE",
    7: ["SK", "SK_Female"],
    8: ["SK_Kosice", "SK_Kosice_Female"]
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


# SITE_CODE_MAPPING = {
#     "UK": {0: 1, 1: 2},
#     "GE": {0: 3, 1: 4},
#     "BE": {0: 5, 1: 6},
#     "SK": {0: 7, 1: 8},
#     "SK_Female": {0: 7, 1: 8},
#     "SK_Kosice": {0: 7, 1: 8},
#     "SK_Kosice_Female": {0: 7, 1: 8},
# }


class MovisensxsValidation:

    def __init__(self, df):
        self.movisensxs_df = df
        self.movisensxs_issues = []

    # Category: MovisensESM
    def validate_visit_country_period_assignation(self, filename):
        def control_filename_structure():

            for code_visit, visit in VALID_TYPE_VISIT_ATTENDANCE.items():
                for code_site, site in SUGGESTION_SITE_CODING_ESM.items():
                    filename_structure = f"IMMERSE_{visit}_{site[0] if isinstance(site, list) else site}"

                    if filename == filename_structure:
                        print(f"\n ✔ | Valid filename for: {filename} & {filename_structure}")
                        correct_site_code = code_site in self.movisensxs_df['SiteCode'].unique()
                        correct_visit_code = code_visit in self.movisensxs_df['VisitCode'].unique()
                        correct_period = code_visit in self.movisensxs_df['period'].unique()

                        print('visit_code: ', code_visit,  self.movisensxs_df['VisitCode'].unique())
                        print('site_code: ', code_site, self.movisensxs_df['SiteCode'].unique())
                        print('period: ', code_visit, self.movisensxs_df['period'].unique())
                        print(correct_site_code, correct_visit_code, correct_period)
                        return filename_structure, correct_site_code, correct_visit_code, correct_period

        valid_filename_structure, valid_site, valid_location, valid_period = control_filename_structure()

        if not valid_filename_structure:
            print(f"\n❌ | Issue found in name: Invalid {filename}")
            self.movisensxs_issues.append(filename)  # Double check

        if not valid_site or valid_location or valid_period:
            print(f"\n❌ | Issue in code assignation:\n "
                  f"SiteCode: {valid_site},\n "
                  f"LocationCode: {valid_location}, \n"
                  f"PeriodCode: {valid_location}, \n")
            self.movisensxs_issues.append(filename)  # Double check