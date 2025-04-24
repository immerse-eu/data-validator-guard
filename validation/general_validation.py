
class DataValidator:

    def __init__(self, df):
        self.df = df
        self.issues = {}

    def check_duplicates(self, subset):
        duplicates = self.df[self.df.duplicated(subset=subset)]
        self.issues["duplicates"] = duplicates
        return duplicates

    def check_typos(self, column, dictionary):
        typos = self.df[~self.df[column].isin(dictionary)]
        self.issues["typos"] = typos
        return typos

    def report(self):
        return self.issues
