
class DataValidator:

    def __init__(self, df):
        self.df = df
        self.issues = {}

    def check_duplicates(self, subset):
        duplicates = self.df[self.df.duplicated(subset=subset)]
        self.issues["duplicates"] = duplicates

        if duplicates.empty:
            print(f"\n ✔ | Validation of duplications passed: No duplications were found in column '{subset}'.")
        else:
            print(f"\n❌ | Duplicated values have been found in column '{subset}':")
        return duplicates

    def check_typos(self, column, dictionary):
        self.df[column] = self.df[column].str.strip().str.lower()
        clean_dictionary = [item.strip().lower() for item in dictionary]

        typos = self.df[~self.df[column].isin(clean_dictionary)]
        self.issues["typos"] = typos

        if  typos.empty:
            print(f"\n ✔ | Validation of typos passed: No typos were found in column '{column}'.")
        else:
            print(f"\n❌ | Typos have been found in column '{column}':")
            print(typos[[column]])
        return typos

    def passed_validation(self):
        return len(self.issues) == 0