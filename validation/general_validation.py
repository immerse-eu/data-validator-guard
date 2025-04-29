
class DataValidator:

    def __init__(self, df):
        self.df = df
        self.issues = {}

    def check_duplicates(self, subset):
        duplicates = self.df[self.df.duplicated(subset=subset)]
        self.issues["duplicates"] = duplicates
        return duplicates

    def check_typos(self, column, dictionary):
        self.df[column] = self.df[column].str.strip().str.lower()
        clean_dictionary = [item.strip().lower() for item in dictionary]

        typos = self.df[~self.df[column].isin(clean_dictionary)]
        self.issues["typos"] = typos

        if not typos.empty:
            print(f"\n❌ | Typos found in column '{column}':")
            print(typos[[column]])
        else:
            print(f"\n ✔ | Validation of typos passed: No typos found in column '{column}'.")
        return typos

    def report(self):
        return self.issues
