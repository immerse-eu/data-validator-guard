import pandas as pd


class DataValidator:

    def __init__(self, df):
        self.df = df
        self.issues = []

    def check_general_duplications(self, subset):
        duplicates = self.df.loc[self.df.duplicated(subset=subset, keep=False)].copy()
        duplicates["issue_type"] = "duplication"

        if duplicates.empty:
            print(f"\n ✔ | Validation of duplications passed: No duplicated rows were found in current table.")
        else:
            print(f"\n❌ | {len(duplicates)} Duplicated values have been found in table:\n '{duplicates}'")
            self.issues.append(duplicates)

    def check_duplications_applying_normalisation(self, column: str = None):
        self.df = self.df.copy()

        if column is not None:
            column = self.df.columns[0]

        def normalize_values_to_uppercase(value):
            if isinstance(value, str):
                return value.strip().upper()
            elif pd.isna(value):
                return value
            else:
                return value

        normalized_column = f"{column}_normalized"
        self.df[normalized_column] = self.df[column].apply(normalize_values_to_uppercase)
        self.df["is_duplicate"] = self.df.duplicated(subset=[normalized_column], keep=False)
        self.df["issue_type"] = "duplication"

        filter_duplicates = self.df[self.df['is_duplicate'] == True]
        # print(self.df[['participant_identifier', 'participant_identifier_normalized', 'is_duplicate']].head(3))

        if filter_duplicates.empty:
            print(f"\n ✔ | Validation of special duplications passed: No duplicated rows were found in current table.")
        else:
            print(f"\n❌ | {len(filter_duplicates)} Duplicated values have been found in current table.")
            self.issues.append(filter_duplicates)

    def check_typos(self, column, dictionary):
        self.df[column] = self.df[column].str.strip().str.lower()
        clean_dictionary = [item.strip().lower() for item in dictionary]

        typos = self.df.loc[~self.df[column].isin(clean_dictionary)].copy()
        typos["issue_type"] = "typo-issue"

        if typos.empty:
            print(f"\n ✔ | Validation of typos passed: No typos were found in column '{column}'.")
        else:
            print(f"\n❌ | {len(typos)} Typos have been found in column '{column}':\n{typos}")
            self.issues.append(typos)

    def check_correct_ids(self, df_control, id_column):
        # self.df[id_column] = self.df[id_column].str.strip()
        # comparison_ids = set(self.df[id_column].dropna())
        # control_ids = set(df_control[id_column].dropna())

        comparison_ids = set(self.df.iloc[:, 0].dropna().astype(str).str.strip())
        control_ids = set(df_control.iloc[:, 0].dropna().astype(str).str.strip())

        print(f"  | Control file length: {len(control_ids)}")
        print(f"  | Test file length: {len(comparison_ids)}")

        missing_ids = control_ids - comparison_ids
        extra_ids = comparison_ids - control_ids

        print(f"  | {len(missing_ids)} Missing IDs in test file: ", list(missing_ids))
        print(f"  | {len(extra_ids)} Extra IDs in test file: ", list(extra_ids))

        # len_discrepancies = self.df[
        #     self.df[id_column].apply(lambda x: not isinstance(x, str) or len(x) != 10 if pd.notnull(x) else False)]
        #
        # print(f"\nNumber of IDs discrepancies: {len(len_discrepancies[id_column])} ")
        # print(len_discrepancies[[id_column, 'site']])

    def report(self):
        if self.issues:
            all_issues_df = pd.concat(self.issues, ignore_index=True)
            print("\n All general issues:", all_issues_df)
        else:
            print("\n Report from general validation process: All validations were successfully passed ✔ !!")

    def passed_validation(self):
        return len(self.issues) == 0
