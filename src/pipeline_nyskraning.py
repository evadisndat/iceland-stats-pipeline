from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

from azure_db import write_to_azure_sql

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

def read_hagstofa_csv(csv_path: Path) -> pd.DataFrame:
    # Hagstofa setur title línu fyrst 
    for skip in (0, 1):
        df = pd.read_csv(csv_path, sep=";", skiprows=skip)
        if df.shape[1] >= 2:
            return df
    return df

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    return df

# Data cleaning
# ----------------------------------------------------------
def coerce_icelandic_numbers(s: pd.Series) -> pd.Series:
    # 1.234 -> 1234, tómt -> NaN
    return pd.to_numeric(
        s.astype(str).str.replace(".", "", regex=False),
        errors="coerce"
    )

def add_year_month_from_monthcode(df: pd.DataFrame, time_col: str) -> pd.DataFrame:
    m = df[time_col].astype(str).str.strip().str.extract(r"^(?P<year>\d{4})[Mm](?P<month>\d{2})$")
    if m.isna().any().any():
        raise ValueError(f"Get ekki lesið mánuðarkóða úr '{time_col}' (dæmi: 2013M01).")
    df["year"] = m["year"].astype(int)
    df["month"] = m["month"].astype(int)
    return df
# ----------------------------------------------------------

def main():
    csv_path = RAW_DIR / "nyskraning.csv"
    if not csv_path.exists():
        print("Settu nyskraning.csv í data/raw fyrst.")
        return

    df = read_hagstofa_csv(csv_path)
    df = normalize_columns(df)

    time_col = df.columns[0] 


    reg_col = "fjöldi_nýskráninga"

    

    # halda bara tíma + nýskráningar
    out = df[[time_col, reg_col]].copy()
    out[reg_col] = coerce_icelandic_numbers(out[reg_col])

    out = add_year_month_from_monthcode(out, time_col=time_col)

    # sía 2013–2023
    out = out[(out["year"] >= 2013) & (out["year"] <= 2023)].reset_index(drop=True)

    out = out.rename(columns={reg_col: "new_registrations"})
    out = out[["year", "month", "new_registrations"]].dropna(subset=["new_registrations"])

    print(out.head())
    print("Rows:", len(out))

    write_to_azure_sql(out, "buisness_registrations_monthly")
    print("Wrote table: buisness_registrations_monthly")

if __name__ == "__main__":
    main()
