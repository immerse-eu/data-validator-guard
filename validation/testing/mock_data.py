import pandas as pd

data = {
    "id": [
        "abc", "a-b-c", "abc_v",  # ID variations (should be the same logically)
        "123", "123",  # Duplicate ID
        "456", "789", "000", "001", "002",
        "xyz", "xyz",  # Duplicate ID
        "789",  # Another duplicate
        "003", "004", "005", "006", "007", "008", "009"
    ],
    "name": [
        "Alice", "Alice", "Alice",  # Same person, maybe typos in ID
        "Bob", "Bob",  # Duplicate entry
        "Charlie", "David", "Eve", "Frank", "Grace",
        "Heidi", "Heidi",  # Duplicate
        "David",  # Duplicate name
        "Ivan", "Judy", "Mallory", "Niaj", "Olivia", "Peggy", "Quinn"
    ],
    "city": [
        "New York", "New York", "New York",  # Valid
        "Londen", "London",  # Typo and correct
        "Paris", "Pariss", "London", "New York", "Londn",  # 2 typos
        "Paris", "Paris",  # Valid
        "Pari",  # Typo
        "New York", "London", "Paris", "Londen", "New York", "Paris", "London"
    ]
}


def generate_df():
    return pd.DataFrame(data)
