from validation.general_validation import DataValidator
from validation.testing import mock_data

VALID_CITIES = {"New York", "London", "Paris"}

ids_test = [] # Paste IDS post processed by GH

def manual_ids_duplication_control():
    control_ids = set()
    for ide in ids_test:
        if ide not in control_ids:
            control_ids.add(ide)
    print("no repetitive items: ", len(control_ids))
    print("length original items", len(ids_test))


def run_test_with_mock_data():

    # -- STEP 1. Read generated "Mock" data
    df = mock_data.generate_df()
    # print(df)

    # -- STEP 2. Run a general validation: typos, and duplicates
    validator = DataValidator(df)
    validator.check_general_duplications(df)
    validator.check_typos(column="city", dictionary=VALID_CITIES)

    # -- STEP 3. Output analysis report
    validator.report()
    # print("\n Report: ", report)

run_test_with_mock_data()
manual_ids_duplication_control()