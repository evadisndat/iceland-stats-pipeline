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

        """
        NOTE (for us):

        All columns in the Azure SQL table are stored as NVARCHAR (text).
        This was done on purpose to avoid errors
        with data types or Icelandic characters. Because of this, SQL treats numbers
        as text.

        To check if the data transferred correctly:

            In Python:
             - Check the number of rows with len(df)
             - This is the expected number of stuff

            In SQL (Azure):
            -  SELECT COUNT(*) FROM population_raw;
            - result should match the Python row count

            In Excel:
            - Load the table from SQL
            - Check the row count at the bottom of the Power Query window

            If the row counts are the same in all three steps, then all data has been
            successfully transferred from CSV ->Python-> SQL ->Excel.
    
        """

        cols_sql = ", ".join([f"[{c}] NVARCHAR(MAX) NULL" for c in df2.columns])
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
