import os
import pandas as pd
import pyodbc
import os
import pandas as pd
import pyodbc

def write_to_azure_sql(df: pd.DataFrame, table_name: str) -> int:
    server = os.environ["AZURE_SQL_SERVER"]
    database = os.environ["AZURE_SQL_DB"]
    username = os.environ["AZURE_SQL_USER"]
    password = os.environ["AZURE_SQL_PASSWORD"]

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

  
    df2 = df.copy()
    df2 = df2.astype(object).where(pd.notna(df2), None)
    for c in df2.columns:
        df2[c] = df2[c].apply(lambda v: None if v is None else str(v))

    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()

        # drop table if exists
        cur.execute(f"IF OBJECT_ID(N'{table_name}', N'U') IS NOT NULL DROP TABLE [{table_name}];")
        conn.commit()

    

        
        numeric_cols = {"year", "month", "unemployed"}

        cols_sql = ", ".join(
            f"[{c}] INT NULL" if c in numeric_cols else f"[{c}] NVARCHAR(MAX) NULL"
            for c in df2.columns
            )
        cur.execute(f"CREATE TABLE [{table_name}] ({cols_sql});")
        conn.commit()

        placeholders = ",".join(["?"] * len(df2.columns))
        col_list = ", ".join([f"[{c}]" for c in df2.columns])
        insert_sql = f"INSERT INTO [{table_name}] ({col_list}) VALUES ({placeholders});"

        cur.fast_executemany = True
        rows = [tuple(r) for r in df2.itertuples(index=False, name=None)]
        cur.executemany(insert_sql, rows)
        conn.commit()

        return cur.execute(f"SELECT COUNT(*) FROM [{table_name}];").fetchone()[0]
