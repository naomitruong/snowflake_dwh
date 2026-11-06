"""Upload one delivery date to MMS_STAGE and load it into VN_MMS_RAW_STOCK.

Run by hand from the project root:
    python -m ingest.load_raw --date 20260818 --skip-upload
"""
import argparse
import csv
import os
import re
from pathlib import Path

import snowflake.connector

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "gcs_download"
SCHEMA = "VN_MMS_RAW_STOCK"
FILES = {
    "CATEGORY": "category",
    "POS": "pos",
    "PRODUCT": "product",
    "PRODUCT_SALES_REPORT": "product_sales_report",
    "PROMOTION": "promotion",
    "PROMOTION_BUNDLE": "promotion_bundle",
    "STORE": "store",
    "VENDOR": "vendor",
}
META_COLUMNS = {"EXECUTE_TARGET_DATE", "SOURCE_FILE", "UPDATED_AT"}


def connect():
    key = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", str(ROOT / "sf_dbt_runner.p8"))
    return snowflake.connector.connect(
        account="GOYCSTU-VG36275",
        user="SVC_DBT_RUNNER",
        private_key_file=key,
        role="DWH_TRANSFORMER",
        warehouse="WH_INGEST",
        database="MSV_DWH_DEV",
        schema=SCHEMA,
    )


def files_ready(date: str) -> bool:
    """True if all 8 files exist, False if none exist, error if only some exist."""
    folder = DATA_DIR / date[:4] / date
    missing = [n for n in FILES.values() if not (folder / f"{n}_{date}.csv").is_file()]
    if len(missing) == len(FILES):
        return False
    if missing:
        raise FileNotFoundError(f"{date}: missing {missing}")
    return True


def upload(date: str) -> None:
    folder = (DATA_DIR / date[:4] / date).resolve()
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            f"PUT 'file://{folder}/*.csv' @MMS_STAGE/{date[:4]}/{date}/ "
            "AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        )
        count = len(cur.fetchall())
        if count != len(FILES):
            raise RuntimeError(f"{date}: uploaded {count} files, expected {len(FILES)}")
        cur.execute("ALTER STAGE MMS_STAGE REFRESH")
    print(f"{date}: uploaded {count} files")


def data_columns(cur, table: str) -> list[str]:
    """CSV columns in table order; stops if the table lacks the metadata columns."""
    cur.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
        (SCHEMA, table),
    )
    columns = [row[0] for row in cur.fetchall()]
    if not META_COLUMNS.issubset(columns):
        raise RuntimeError(f"{table}: missing {sorted(META_COLUMNS - set(columns))}")
    return [c for c in columns if c not in META_COLUMNS]

def check_header(date: str, table: str, name: str, columns: list[str]) -> None:
    """Stop if the CSV header no longer matches the table, since COPY maps by position."""
    path = DATA_DIR / date[:4] / date / f"{name}_{date}.csv"
    with open(path, encoding="utf-8-sig", errors="replace", newline="") as f:
        header = next(csv.reader(f))
    actual = [re.sub(r"[^A-Z0-9]+", "_", h.strip().upper()).strip("_") for h in header]
    if actual != columns:
        raise RuntimeError(f"{table}: header changed. expected={columns} actual={actual}")


def load(date: str) -> None:
    """Replace one date in all 8 tables, in one transaction (all or nothing)."""
    iso = f"{date[:4]}-{date[4:6]}-{date[6:]}"
    with connect() as conn, conn.cursor() as cur:
        columns = {table: data_columns(cur, table) for table in FILES}
        for table, name in FILES.items():
            check_header(date, table, name, columns[table])
        cur.execute("BEGIN")
        try:
            for table, name in FILES.items():
                cols = ", ".join(f'"{c}"' for c in columns[table])
                positions = ", ".join(f"${i}" for i in range(1, len(columns[table]) + 1))
                cur.execute(f"DELETE FROM {table} WHERE EXECUTE_TARGET_DATE = '{iso}'")
                cur.execute(
                    f"COPY INTO {table} ({cols}, EXECUTE_TARGET_DATE, SOURCE_FILE, UPDATED_AT) "
                    f"FROM (SELECT {positions}, '{iso}'::DATE, METADATA$FILENAME, CURRENT_TIMESTAMP() "
                    f"FROM @MMS_STAGE/{date[:4]}/{date}/) "
                    f"FILES = ('{name}_{date}.csv.gz') "
                    "FILE_FORMAT = (FORMAT_NAME = CSV_RAW) "
                    "ON_ERROR = ABORT_STATEMENT FORCE = TRUE"
                )
                result = cur.fetchall()
                # COPY returns a one-column message when the file is not on the stage
                if len(result) != 1 or len(result[0]) < 4 or not result[0][3]:
                    raise RuntimeError(f"{table}: nothing loaded for {date}: {result}")
                print(f"{table}: {result[0][3]} rows")
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYYMMDD")
    parser.add_argument("--skip-upload", action="store_true")
    args = parser.parse_args()
    if not args.skip_upload:
        upload(args.date)
    load(args.date)
