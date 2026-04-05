"""Bootstrap wcmktvsj Turso remote database with seed data from wcmktvsj_model.db."""

import sqlite3
import os
import libsql
from dotenv import load_dotenv

load_dotenv()

TURSO_URL = os.getenv("TURSO_WCMKTVSJ_URL")
TURSO_TOKEN = os.getenv("TURSO_WCMKTVSJ_TOKEN")
MODEL_DB = "wcmktvsj_model.db"

SEED_TABLES = [
    "watchlist",
    "doctrine_fits",
    "ship_targets",
    "doctrine_map",
    "lead_ships",
    "module_equivalents",
    "doctrines",
]


def main():
    src = sqlite3.connect(MODEL_DB)
    src.row_factory = sqlite3.Row

    print(f"Connecting to Turso: {TURSO_URL}")
    remote = libsql.connect(TURSO_URL, auth_token=TURSO_TOKEN)

    # Verify tables exist
    tables = [t[0] for t in remote.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    print(f"Remote tables: {tables}")

    # Insert seed data
    print("\nInserting seed data...")
    for table in SEED_TABLES:
        rows = src.execute(f"SELECT * FROM {table}").fetchall()
        if not rows:
            print(f"  {table}: 0 rows (skipping)")
            continue

        columns = rows[0].keys()
        col_names = ", ".join(columns)
        placeholders = ", ".join(["?" for _ in columns])
        insert_sql = f"INSERT OR REPLACE INTO {table} ({col_names}) VALUES ({placeholders})"

        for row in rows:
            remote.execute(insert_sql, tuple(row))
        remote.commit()
        print(f"  {table}: {len(rows)} rows inserted")

    # Verify
    print("\nVerification:")
    for table in SEED_TABLES:
        count = remote.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count} rows")

    src.close()
    remote.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
