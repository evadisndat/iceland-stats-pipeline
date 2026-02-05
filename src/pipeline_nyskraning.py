from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import os
from azure_db import write_to_azure_sql

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"

def read_hagstofa_csv(csv_path: Path) -> pd.DataFrame:
    for skip in (0, 1):
        df = pd.read_csv(csv_path, sep=";", skiprows=skip)
        if df.shape[1] >= 2:
            return df
    return df

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    return df

def coerce_icelandic_numbers(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(".", "", regex=False), errors="coerce")

def add_year_month_from_monthcode(df: pd.DataFrame, time_col: str) -> pd.DataFrame:
    m = df[time_col].astype(str).str.strip().str.extract(r"^(?P<year>\d{4})[Mm](?P<month>\d{2})$")
    if m.isna().any().any():
        raise ValueError(f"Get ekki lesið mánuðarkóða úr '{time_col}'.")
    df["year"] = m["year"].astype(int)
    df["month_int"] = m["month"].astype(int) # Rename slightly to avoid confusion with our date column
    return df

def main():
    csv_path = RAW_DIR / "nyskraning.csv"
    if not csv_path.exists():
        print("Settu nyskraning.csv í data/raw fyrst.")
        return

    df = read_hagstofa_csv(csv_path)
    df = normalize_columns(df)

    time_col = df.columns[0] 
    reg_col = "fjöldi_nýskráninga"

    # Clean and parse
    out = df[[time_col, reg_col]].copy()
    out[reg_col] = coerce_icelandic_numbers(out[reg_col])
    out = add_year_month_from_monthcode(out, time_col=time_col)

    # Filter 2013–2023 and Sort Newest First
    out = out[(out["year"] >= 2013) & (out["year"] <= 2023)]
    out = out.sort_values(by=["year", "month_int"], ascending=False)

    # Create the proper SQL DATE (YYYY-MM-01)
    out["mánuður"] = pd.to_datetime(
        out["year"].astype(str) + "-" + out["month_int"].astype(str) + "-01"
    ).dt.strftime('%Y-%m-%d')

    # Final Prep
    out = out.rename(columns={reg_col: "nýskráningar"})
    # Keep only the Date and the Value
    final_out = out[["mánuður", "nýskráningar"]].dropna(subset=["nýskráningar"]).reset_index(drop=True)

    print("--- PREVIEW (Nýskráningar) ---")
    print(final_out.head())
    
    count = write_to_azure_sql(final_out, "business_registrations_monthly")
    print(f"Success! {count} rows in business_registrations_monthly")

if __name__ == "__main__":
    main()