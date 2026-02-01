from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
load_dotenv()
import os
from azure_db import write_to_azure_sql





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
def add_year_month_from_monthcode(df: pd.DataFrame, time_col: str) -> pd.DataFrame:
    s = df[time_col].astype(str).str.strip()
    m = s.str.extract(r"^(?P<year>\d{4})[Mm](?P<month>\d{2})$")
    if m.isna().any().any():
        raise ValueError(f"Get ekki lesið mánuð úr dálknum '{time_col}'.")
    df["year"] = m["year"].astype(int)
    df["month"] = m["month"].astype(int)
    return df


def main():
    
    csv_path = RAW_DIR / "atvinnuleysi.csv"
    if not csv_path.exists():
        print("Settu atvinnuleysi.csv í data/raw fyrst.")
        return

    df = read_hagstofa_csv(csv_path)
    df = normalize_columns(df)

  
    time_col = df.columns[0]  


    preferred = "alls_atvinnulausir"
    if preferred not in df.columns:
        candidates = [c for c in df.columns if "atvinnulaus" in c and "alls" in c]
        print("Fann ekki 'alls_atvinnulausir'. Kandidatar:", candidates)
        if not candidates:
            raise KeyError("Enginn dálkur fannst sem inniheldur bæði 'alls' og 'atvinnulaus'.")
        unemp_col = candidates[0]
    else:
        unemp_col = preferred

   
    df = df[[time_col, unemp_col]]

    
    df = coerce_icelandic_numbers(df, exclude_cols={time_col})

    
    df = add_year_month_from_monthcode(df, time_col=time_col)

    
    df = df[(df["year"] >= 2013) & (df["year"] <= 2023)].reset_index(drop=True)

   
    df = df.rename(columns={unemp_col: "unemployed"})

    
    out = df[["year", "month", "unemployed"]].dropna(subset=["unemployed"])

    print(out.head())
    print("Rows:", len(out))

    print("\nWriting to Azure SQL...")
    count = write_to_azure_sql(out, "unemployment_monthly")
    print("Rows in Azure SQL unemployment_monthly:", count)


if __name__ == "__main__":
    main()
