from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import os

from azure_db import write_to_azure_sql

load_dotenv()

# File path set up 
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

#downloaded semmikommuskipt csv með table names
def read_hagstofa_csv(csv_path: Path) -> pd.DataFrame:
    for skip in (0, 1):
        df = pd.read_csv(csv_path, sep=";", skiprows=skip)
        if df.shape[1] >= 2:
            return df
    return df

# Data cleaning
# ----------------------------------------------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
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



#parse dates from pbX formati í venjulegar dagsetningar
"""
   time_col
0   2013M01
1   2013M02
2   2013M03

   time_col  year  month
0   2013M01  2013      1
1   2013M02  2013      2
2   2013M03  2013      3
"""
def add_year_month_from_monthcode(df: pd.DataFrame, time_col: str) -> pd.DataFrame:
    #regex that looks for year and month to add 
    s = df[time_col].astype(str).str.strip()
    m = s.str.extract(r"^(?P<year>\d{4})[Mm](?P<month>\d{2})$")
    if m.isna().any().any():
        raise ValueError(f"Get ekki lesið mánuð úr dálknum '{time_col}'.")
    df["year"] = m["year"].astype(int)
    df["month"] = m["month"].astype(int)
    return df
# ----------------------------------------------------------



def main():
    csv_path = RAW_DIR / "atvinnuleysi.csv"
    if not csv_path.exists():
        print(f"Missing file: {csv_path}")
        return

    # read csv and normalize data
    df = read_hagstofa_csv(csv_path)
    df = normalize_columns(df)

    #find the number of unemployed
    time_col = df.columns[0]  
    preferred = "alls_atvinnulausir"
    
    if preferred not in df.columns:
        candidates = [c for c in df.columns if "atvinnulaus" in c and "alls" in c]
        if not candidates:
            raise KeyError("Could not find a matching unemployment column.")
        unemp_col = candidates[0]
    else:
        unemp_col = preferred

    #Transforms data into correct types for SQL
    df = df[[time_col, unemp_col]]
    df = coerce_icelandic_numbers(df, exclude_cols={time_col})
    df = add_year_month_from_monthcode(df, time_col=time_col)

    #filter and sort data
    df = df[(df["year"] >= 2013) & (df["year"] <= 2023)]
    df = df.sort_values(by=["year", "month"], ascending=False)

    #Createa SQL data
    #Instead of 2023 and 01, we create "2023-01-01", adds a 01 to end of every month.
    # This is for ease of use with other tools that use the date format. 
    df["mánuður"] = pd.to_datetime(
        df["year"].astype(str) + "-" + df["month"].astype(str) + "-01"
    ).dt.strftime('%Y-%m-%d')

    #FINAL PREP
    df = df.rename(columns={unemp_col: "atvinnulausir"})
    out = df[["mánuður", "atvinnulausir"]].dropna(subset=["atvinnulausir"]).reset_index(drop=True)

    print("--- PREVIEW ---")
    print(out.head())
    print(f"Total rows to upload: {len(out)}")

    print("\nConnecting to Azure...")
    count = write_to_azure_sql(out, "unemployment_monthly")
    print(f"Success! {count} rows are now in Azure.")

if __name__ == "__main__":
    main()