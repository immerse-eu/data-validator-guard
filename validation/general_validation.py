import re
import pandas as pd

VALID_CENTER_ACRONYMS = ["BI", "LE", "MA", "WI", "BR", "KO", "CA", "LO"]
VALID_PARTICIPANT_TYPES = ["P", "C", "A"]

VALID_PATTERN_USING_MINUS = re.compile(r"^I-(" + "|".join(VALID_CENTER_ACRONYMS) + r")-(" + "|".join(VALID_PARTICIPANT_TYPES) + r")-\d{3}$")
VALID_PATTERN_USING_UNDERSCORE = re.compile(r"^I_(" + "|".join(["WI", "MA"]) + r")_(" + "|".join(VALID_PARTICIPANT_TYPES) + r")_\d{3}$")


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
            self.issues.append(duplicates[["participant_identifier", "issue_type"]])

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

        if filter_duplicates.empty:
            print(f"\n ✔ | Validation of special duplications passed: No duplicated rows were found in current table.")
        else:
            print(
                f"\n❌ | {len(filter_duplicates)} Duplicated values using normalization have been found in current table.")
            self.issues.append(filter_duplicates[['participant_identifier', 'issue_type']])

    def check_typos(self, column, dictionary):
        self.df[column] = self.df[column].str.strip().str.lower()
        clean_dictionary = [item.strip().lower() for item in dictionary]

        typos = self.df.loc[~self.df[column].isin(clean_dictionary)].copy()
        typos["issue_type"] = "typo"

        if typos.empty:
            print(f"\n ✔ | Validation of typos passed: No typos were found in column '{column}'.")
        else:
            print(f"\n❌ | {len(typos)} Typos have been found in column '{column}':\n{typos}")
            self.issues.append(typos[['participant_identifier', 'issue_type']])

    def check_typos_in_ids(self, id_column):
        id_validation = self.df.copy()

        def validate_id(idx):
            if isinstance(idx, str):
                if VALID_PATTERN_USING_MINUS.match(idx) or VALID_PATTERN_USING_UNDERSCORE.match(idx):
                    return 'valid_id'
                else:
                    return 'invalid_id_pattern'

        for idx in id_validation:
            if isinstance(idx, str):
                validate_id(idx)

        id_validation['issue_type'] = id_validation.iloc[:, id_column].apply(validate_id)
        filter_issues = id_validation[id_validation['issue_type'] == 'invalid_id_pattern']

        if filter_issues.empty:
            print(f"\n ✔ | Validation of typos passed: No typos were found in column '{id_column}'.")
        else:
            print(f"\n❌ | {len(filter_issues['issue_type'])} Typos have been found in IDs")
            self.issues.append(filter_issues[['participant_identifier', 'issue_type']])
            # print(self.issues)

    def compare_ids_with_redcap_ids(self, df_control, id_column):
        comparison_ids = set(self.df.iloc[:, 0].dropna().astype(str).str.strip())
        control_redcap_ids = set(df_control.iloc[:, 0].dropna().astype(str).str.strip())

        print(f"  | Total Reference ids length: {len(control_redcap_ids)}")
        print(f"  | Current test file length: {len(comparison_ids)}")

        missing_ids = control_redcap_ids - comparison_ids
        extra_ids = comparison_ids - control_redcap_ids

        print(f"  | {len(missing_ids)} Missing IDs in test file: ", list(missing_ids))
        print(f"  | {len(extra_ids)} Extra IDs in test file: ", list(extra_ids))

        if extra_ids:
            extra_ids = self.df[self.df["participant_identifier"].astype(str).str.strip().isin(extra_ids)].copy()
            extra_ids["issue_type"] = "unknown_id_in_reference"
            self.issues.append(extra_ids[["participant_identifier", "issue_type"]])

    def report(self, export_path, filename):
        if self.issues:
            all_issues_df = pd.concat(self.issues, ignore_index=True)
            all_issues_df['issue_type'] = all_issues_df['issue_type'].astype(str)
            grouped_issues = (
                all_issues_df
                .groupby("participant_identifier", as_index=False)
                .agg({"issue_type": lambda x: ", ".join(sorted(set(x)))})
            )
            grouped_issues = grouped_issues.sort_values(by=["issue_type", "participant_identifier"], ascending=True)
            # grouped_issues.to_csv(os.path.join(export_path, f"new_issues_{filename}"), index=False)
            print(f"\n All general issues exported as: {f'issues_{filename}'}")
            return grouped_issues
        else:
            print("\n Report from general validation process: All validations were successfully passed ✔ !!")

    def passed_validation(self):
        return len(self.issues) == 0
