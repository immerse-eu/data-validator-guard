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
        self.movisensxs = df
        self.movisensxs_issues = []

    # Category: MovisensESM
    def validate_visit_and_country_assignation(self, filename):
        # TODO: 1 Validate that the Table' name fits with the data. Example: IMMERSE_[VISIT]_[COUNTRY]. check functionality.
        filename_stuctrure = f"IMMERSE_{VALID_TYPE_VISIT_ATTENDANCE.values()}_{LOCATION_NAMES_ESM.values()}"
        if filename in filename_stuctrure:
            print(filename_stuctrure)
