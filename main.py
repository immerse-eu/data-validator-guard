from validation.seach_values import execute_search
from validation.testing import mock_data
from validation.general_validation import DataValidator

VALID_CITIES = {"New York", "London", "Paris"}

def main():

    print("Main Data Validator")

    # -- STEP 1. Generate "Mock" data
    df = mock_data.generate_df()
    print(df)

    # -- STEP 2. Run a general validation: typos, and duplicates
    validator = DataValidator(df)

    duplicates = validator.check_duplicates(df)
    print("\n Duplicates found: ", duplicates)

    typos = validator.check_typos(column="city", dictionary=VALID_CITIES)
    print("\n Typos found: ", typos)

    report = validator.report()
    print("\n Report: ", report)

    # -- EXTRA ACTION: SEARCH
    # input_value = ['ABC', 'CBA']        # TODO: Change these values for real IDs or value to search.
    # execute_search(input_value)

if __name__ == "__main__":
    main()
