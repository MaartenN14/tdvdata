import pyodbc
import pandas as pd
from pathlib import Path
from datetime import date as dt_date
from tqdm import tqdm


# =========================
# === CONFIGURATION
# =========================
REPORT_DATE = 20181231  # yyyymmdd as int
OUTPUT_DIR = Path("output")

QUERIES = {
    "position_data": {
        "table": '"TDVDatabase"."OPD"."ExternalReportingClosedLimitHist"',
        "date_col": '"AsOfDate"',
        "date_type": "int",
        "filename_prefix": "position_data",
    },
    "performance_net": {
        "table": '"TDVDatabase"."Performance"."PearlMonthlyClientPerformance"',
        "date_col": "reportdate",
        "date_type": "date",
        "filename_prefix": "performance_net",
    },
}


# =========================
# === HELPERS
# =========================
def to_python_date(yyyymmdd: int) -> dt_date:
    s = str(yyyymmdd)
    return dt_date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def tdv_connect():
    conn = pyodbc.connect("DSN=TDV PRD;", autocommit=True)
    conn.setdecoding(pyodbc.SQL_CHAR, "latin-1")
    conn.setdecoding(pyodbc.SQL_WCHAR, "utf-16le")
    conn.setencoding("latin-1")
    return conn


def run_query(query: str, params=()):
    conn = tdv_connect()
    cursor = conn.cursor()

    print("\n--- Executing SQL ---")
    print(query)
    print("Params:", params)
    print("---------------------")

    try:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        cols = [c[0] for c in cursor.description]
        return pd.DataFrame.from_records(rows, columns=cols)
    finally:
        cursor.close()
        conn.close()


# =========================
# === DATE FILTER LOGIC
# =========================
def build_date_param(date_type):
    if date_type == "date":
        return to_python_date(REPORT_DATE)
    elif date_type == "int":
        return int(REPORT_DATE)
    else:
        raise ValueError(f"Unknown date_type: {date_type}")


def get_distinct_dates(table, date_col, date_type):
    threshold = build_date_param(date_type)

    q = f"""
        SELECT DISTINCT {date_col}
        FROM {table}
        WHERE {date_col} >= ?
        ORDER BY {date_col}
    """

    df = run_query(q, params=(threshold,))
    return df.iloc[:, 0].tolist()


def fetch_partition(table, date_col, date_type, d):
    if date_type == "date":
        if hasattr(d, "date"):
            d = d.date()
    elif date_type == "int":
        d = int(d)

    q = f"""
        SELECT *
        FROM {table}
        WHERE {date_col} = ?
    """

    return run_query(q, params=(d,))


# =========================
# === MAIN PIPELINE
# =========================
def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    for name, meta in QUERIES.items():
        table = meta["table"]
        date_col = meta["date_col"]
        date_type = meta["date_type"]
        prefix = meta["filename_prefix"]

        subdir = OUTPUT_DIR / prefix
        subdir.mkdir(exist_ok=True)

        print(f"\n=== Exporting: {name} ===")
        print(f"Table         : {table}")
        print(f"Date column   : {date_col} ({date_type})")
        print(f"Output folder : {subdir}")

        # 1. Get distinct dates
        dates = get_distinct_dates(table, date_col, date_type)
        print(f"  Found {len(dates)} dates")

        if not dates:
            print("  [WARNING] No rows found after date filtering.")
            continue

        # 2. Export with progress bar
        for d in tqdm(dates, desc=f"Processing {name}", unit="date"):
            ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
            out_path = subdir / f"{prefix}_{ds}.parquet"

            # Skip existing files
            if out_path.exists():
                tqdm.write(f"  {ds}: SKIPPED (already exists)")
                continue

            df = fetch_partition(table, date_col, date_type, d)
            df.to_parquet(out_path, index=False)

            tqdm.write(f"  {ds}: {len(df)} rows → {out_path}")


# =========================
# === ENTRY POINT
# =========================
if __name__ == "__main__":
    main()