from validation.general_validation import DataValidator
from validation.testing import mock_data

VALID_CITIES = {"New York", "London", "Paris"}


def run_test_with_mock_data():

    # -- STEP 1. Read generated "Mock" data
    df = mock_data.generate_df()
    print(df)

    # -- STEP 2. Run a general validation: typos, and duplicates
    validator = DataValidator(df)
    validator.check_duplicates(df)
    validator.check_typos(column="city", dictionary=VALID_CITIES)

    # -- STEP 3. Output analysis report
    report = validator.report()
    print("\n Report: ", report)