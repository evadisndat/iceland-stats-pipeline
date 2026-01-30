from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
load_dotenv()
import os
from azure_db import write_to_azure_sql


"""
Loads population data downloaded from Hagstofan.

- Reads semicolon-separated CSV exported 
- Standardizes column names
- Converts numbers like 122.342 to 122342
- Identifies the year column 
- Since population data is published annually the dataset is expanded
  to monthly values by assuming constant population within each year.
- Calculates the number of foreigners as
    foreigners = total population - population born in Iceland.
- Reorders columns 
- Writes the processed dataset to Azure SQL, replacing the table if it exists.

"""




BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

#downloaded semmikommuskipt csv
def read_hagstofa_csv(csv_path: Path) -> pd.DataFrame:
    for skip in (0, 1):
        df = pd.read_csv(csv_path, sep=";", skiprows=skip)
        if df.shape[1] >= 2:
            return df
    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    return df


def find_year_col(df: pd.DataFrame) -> str:

    for c in df.columns:
        s = df[c].astype(str).str.strip()
        if s.str.fullmatch(r"\d{4}").any():
            return c
    raise ValueError("Could not find a year column ")


def add_year_month_from_yearcol(df: pd.DataFrame, year_col: str) -> pd.DataFrame:
    """
   aðeins mælt 1x á ári þannig breytum ári í mánuð
    """
    s = df[year_col].astype(str).str.strip()
    y = s.str.extract(r"^(?P<year>\d{4})$")
    if not y["year"].notna().any():
        raise ValueError(f"Year column '{year_col}' did not contain 4-digit years.")
    df["year"] = pd.to_numeric(y["year"], errors="raise").astype(int)
    df["month"] = 1
    return df


def coerce_icelandic_numbers(df: pd.DataFrame, exclude_cols: set) -> pd.DataFrame:
    for c in df.columns:
        if c in exclude_cols:
            continue
        df[c] = (
            df[c]
            .astype(str)
            .str.replace(".", "", regex=False)  # 23.400 -> 23400
            .replace("..", None)
        )
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def load_population_table(csv_path: Path) -> pd.DataFrame:
    """
    Sérhæft fyrir mannfjölda (ár + aldur + alls + ísland).
    """
    df = read_hagstofa_csv(csv_path)
    df = normalize_columns(df)

    year_col = find_year_col(df)

    
    exclude = {year_col}
    
    exclude.add(df.columns[0])

    df = coerce_icelandic_numbers(df, exclude_cols=exclude)
    df = add_year_month_from_yearcol(df, year_col)

    return df


def expand_population_to_months(df: pd.DataFrame) -> pd.DataFrame:
    """
  Population data is published annually  and was therefore expanded to
    monthly frequency by assuming constant population within each year. 

    """
    rows = []

    for _, row in df.iterrows():
        for m in range(1, 13):
            new_row = row.copy()
            new_row["month"] = m
            rows.append(new_row)

    return pd.DataFrame(rows)


def main():
    a_path = RAW_DIR / "mannfjoldi.csv"
    if not a_path.exists():
        print("Put mannfjoldi.csv in data/raw first.")
        return

    df = load_population_table(a_path)


    #reikna fjölda innflytjenda með fjöldi alls-island
    df["foreigners"] = df["alls_alls"] - df["ísland_alls"]

    cols = list(df.columns)
    idx = cols.index("ísland_alls") + 1
    cols.insert(idx, cols.pop(cols.index("foreigners")))
    df = df[cols]

    # droppa ár því við öddum year, þarf að laga logic 
    if "ár" in df.columns:
        df = df.drop(columns=["ár"])

    df = expand_population_to_months(df)

    print("Columns:", list(df.columns))
    print(df.head(5))
    print("Rows:", len(df))

    print("\nWriting to Azure SQL...")
    count = write_to_azure_sql(df, "population_raw")
    print("Rows in Azure SQL population_raw:", count)
    print("Database:", os.environ["AZURE_SQL_DB"], "on", os.environ["AZURE_SQL_SERVER"])


if __name__ == "__main__":
    main()
