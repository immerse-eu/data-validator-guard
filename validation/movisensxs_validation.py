VALID_TYPE_VISIT_ATTENDANCE = {
    0: 'T0',  # Baseline
    1: 'T1',
    2: 'T2',
    3: 'T3',
}

LOCATION_NAMES_ESM = {
    0: "BE",
    1: "UK",
    2: "GE",
    3: "SK",
    4: "SK_Female",
    5: "SK_Kosice",
    6: "SK_Kosice_Female"
}


class MovisensxsValidation:

    def __init__(self, df):
        self.movisensxs_df = df
        self.movisensxs_issues = []

    # Category: MovisensESM
    def validate_visit_and_country_assignation(self, filename):
        def control_filename_structure():
            for visit in VALID_TYPE_VISIT_ATTENDANCE.values():
                for location in LOCATION_NAMES_ESM.values():
                    filename_structure = f"IMMERSE_{visit}_{location}"
                    if filename == filename_structure:
                        print(f"\n ✔ | Valid filename for: {filename} & {filename_structure}")
                        correct_site = visit in self.movisensxs_df['SiteCode'].unique()
                        correct_location = location in self.movisensxs_df['SiteCode'].unique()
                        return filename_structure, correct_site, correct_location

        valid_filename_structure, valid_site, valid_location = control_filename_structure()
        if not valid_filename_structure:
            print(f"\n❌ | Issue found in name: Invalid {filename}")
            self.movisensxs_issues.append(filename)  # Double check

        if not valid_site and valid_location:
            print(f"\n❌ | Issue in code assignation: SiteCode: {valid_site},  LocationCode: {valid_location}")
            self.movisensxs_issues.append(filename) # Double check