from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
load_dotenv()
import os
from azure_db import write_to_azure_sql




"""
loads hagstofa csv - assuming you downloaded semicolon separated
CSV rotated clockwise, handles titles at the top by skipping first line.

Standardizes the table by making column names consistent and cleans up
floatnumbers to int, turns missing values into NaN (didn't know how we were
supposed to handle those yet).

Creates year and month.

Parses PXWeb format time values (yyyyMx) = year = YYYY month = x.

Prints the columns list og fyrstu 3 time/year/month rows & total row count.

Writes the cleaned data into SQL - creates or updates db/project.db (SQLite).
Saves a SQL table called dataset_a containing the cleaned data.
And prints number of rows inserted + database location.
Currently it runs on a single CSV file called from main() 
"""




BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
DB_PATH = BASE_DIR / "db" / "project.db"

RAW_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)



def read_hagstofa_csv(csv_path: Path) -> pd.DataFrame:
    # assuming first line is a title since we cannot edit anything by hand
    for skip in (0, 1):
        df = pd.read_csv(csv_path, sep=";", skiprows=skip)
        # if we got at least 2 columns, it's probably correct
        if df.shape[1] >= 2:
            return df
    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    return df


def find_time_col(df: pd.DataFrame) -> str:
    candidates = ["time", "tími", "timi", "timabil", "tímabil", "date", "mánuður", "manudur"]
    for c in candidates:
        if c in df.columns:
            return c
    # if not then the first column usually holds time in rotated tables
    return df.columns[0]


def add_year_month(df: pd.DataFrame, time_col: str) -> pd.DataFrame:
    s = df[time_col].astype(str).str.strip()

    # PXWeb: 2020M01 - hagstofan notar thad format
    m = s.str.extract(r"^(?P<year>\d{4})M(?P<month>\d{1,2})$")
    if m.notna().all(axis=1).any():
        df["year"] = pd.to_numeric(m["year"], errors="raise").astype(int)
        df["month"] = pd.to_numeric(m["month"], errors="raise").astype(int)
        return df

    # YYYY-MM or YYYY/MM or YYYY.MM eda full date
    dt = pd.to_datetime(s, errors="coerce")
    if dt.notna().any():
        df["year"] = dt.dt.year
        df["month"] = dt.dt.month
        if df["year"].isna().any() or df["month"].isna().any():
            raise ValueError(f"Some '{time_col}' rows could not be parsed as dates.")
        return df

    raise ValueError(f"Could not parse time column '{time_col}' into year/month.")


def coerce_icelandic_numbers(df: pd.DataFrame, exclude_cols: set) -> pd.DataFrame:
    for c in df.columns:
        if c in exclude_cols:
            continue
        df[c] = (
            df[c]
            .astype(str)
            .str.replace(".", "", regex=False)  #t.d 159.248 -> 159248
            .replace("..", None)
        )
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def load_table(csv_path: Path) -> pd.DataFrame:
    df = read_hagstofa_csv(csv_path)
    df = normalize_columns(df)

    time_col = find_time_col(df)
    df = df.rename(columns={time_col: "time"})

    df = coerce_icelandic_numbers(df, exclude_cols={"time"})
    df = add_year_month(df, "time")

    return df




def main():
    a_path = RAW_DIR / "dataset3.csv"  #change this for other datasets
    if not a_path.exists():
        print("Put dataset you are using in data/raw first.")
        return

    df_a = load_table(a_path)

    print("Columns:", list(df_a.columns))
    print(df_a.head(3)[["time", "year", "month"]])
    print("Rows:", len(df_a))

    print("\nWriting to Azure SQL...")
    count = write_to_azure_sql(df_a, "dataset_a")
    print("Rows in Azure SQL dataset_a:", count)
    print("Database:", os.environ["AZURE_SQL_DB"], "on", os.environ["AZURE_SQL_SERVER"])



if __name__ == "__main__":
    main()
