from validation.seach_values import execute_search
from validation.testing import mock_data

def main():

    print("Main Data Validator")

    #-- EXTRA ACTION: SEARCH
    input_value = ['ABC', 'CBA']        # TODO: Change these values for real IDs or value to search.
    execute_search(input_value)

if __name__ == "__main__":
    main()
