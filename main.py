import search_values


def main():

    print("Main Data Validator")

    # ACTION 1: Search IDs or any other value
    input_value = list(input("Please enter value(s) to search in a list form"))
    search_values.execute_search(input_value)


if __name__ == "__main__":
    main()
