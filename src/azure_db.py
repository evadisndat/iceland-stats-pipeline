import os
import pandas as pd
import pyodbc

def write_to_azure_sql(df: pd.DataFrame, table_name: str) -> int:
    #Connects to azure database using credentials from .env
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

    #Convert NaNs to None so SQL recognizes them as NULL
    df_clean = df.astype(object).where(pd.notna(df), None)

    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()

        #Deletes table if it exists (Scorched Earth Policy)
        cur.execute(f"IF OBJECT_ID(N'{table_name}', N'U') IS NOT NULL DROP TABLE [{table_name}];")
        conn.commit()

        # Define column types for SQL
        numeric_cols = {"atvinnulausir", "nýskráningar"}
        date_cols = {"mánuður"}

        cols_sql_list = []
        for c in df_clean.columns:
            if c in numeric_cols:
                cols_sql_list.append(f"[{c}] INT NULL")
            elif c in date_cols:
                cols_sql_list.append(f"[{c}] DATE NULL")
            else:
                cols_sql_list.append(f"[{c}] NVARCHAR(MAX) NULL")

        cols_sql = ", ".join(cols_sql_list)
        
        #Create the Table
        cur.execute(f"CREATE TABLE [{table_name}] ({cols_sql});")
        conn.commit()

        #Insertion of data
        placeholders = ",".join(["?"] * len(df_clean.columns))
        col_list = ", ".join([f"[{c}]" for c in df_clean.columns])
        insert_sql = f"INSERT INTO [{table_name}] ({col_list}) VALUES ({placeholders});"

        #Azure magic to make it go fast (Batch uploads)
        cur.fast_executemany = True
        rows = [tuple(r) for r in df_clean.itertuples(index=False, name=None)]
        cur.executemany(insert_sql, rows)
        conn.commit()

        #Return row count for the local print statement
        return cur.execute(f"SELECT COUNT(*) FROM [{table_name}];").fetchone()[0]