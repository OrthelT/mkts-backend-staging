#!/usr/bin/env python3
"""
Script to update null timestamps in database tables with current UTC time.

This script checks all tables with timestamp columns and updates any null values
with the current UTC time to prevent datetime conversion errors.
"""

import sys
from datetime import datetime, timezone
from sqlalchemy import text, create_engine
from src.mkts_backend.config.config import DatabaseConfig
from src.mkts_backend.config.logging_config import configure_logging

logger = configure_logging(__name__)

# Tables and their timestamp column names
TABLES_TO_FIX = [
    ("doctrines", "timestamp"),
    ("marketorders", "issued"),
    ("market_history", "timestamp"),
    ("marketstats", "last_update"),
    ("region_orders", "timestamp"),
    ("region_history", "timestamp"),
    ("jita_history", "timestamp"),
    ("updatelog", "timestamp"),
]

def fix_null_timestamps(dry_run: bool = False):
    """
    Update null timestamps in all relevant tables.

    Args:
        dry_run: If True, only report what would be updated without making changes
    """
    db = DatabaseConfig("wcmkt")
    engine = db.engine

    total_updated = 0
    current_time = datetime.now(timezone.utc).replace(tzinfo=None)

    logger.info(f"Starting timestamp fix (dry_run={dry_run})")
    logger.info(f"Current UTC time: {current_time}")
    print(f"\n{'DRY RUN - ' if dry_run else ''}Fixing null timestamps in database")
    print(f"Current UTC time: {current_time}")
    print("=" * 70)

    with engine.connect() as conn:
        for table_name, timestamp_column in TABLES_TO_FIX:
            try:
                # Check if table exists
                check_query = text(f"""
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name=:table_name
                """)
                result = conn.execute(check_query, {"table_name": table_name})
                if not result.fetchone():
                    logger.warning(f"Table {table_name} does not exist, skipping...")
                    print(f"⚠️  Table '{table_name}' not found - skipping")
                    continue

                # Count null timestamps
                count_query = text(f"""
                    SELECT COUNT(*)
                    FROM {table_name}
                    WHERE {timestamp_column} IS NULL
                """)
                result = conn.execute(count_query)
                null_count = result.fetchone()[0]

                if null_count == 0:
                    print(f"✓  {table_name}.{timestamp_column}: No null values found")
                    continue

                print(f"→  {table_name}.{timestamp_column}: {null_count} null values found")

                if not dry_run:
                    # Update null timestamps
                    update_query = text(f"""
                        UPDATE {table_name}
                        SET {timestamp_column} = :current_time
                        WHERE {timestamp_column} IS NULL
                    """)
                    conn.execute(update_query, {"current_time": current_time})
                    conn.commit()

                    # Verify update
                    verify_result = conn.execute(count_query)
                    remaining_nulls = verify_result.fetchone()[0]

                    if remaining_nulls == 0:
                        print(f"   ✓ Updated {null_count} rows successfully")
                        total_updated += null_count
                    else:
                        print(f"   ⚠️  Warning: {remaining_nulls} null values remain")
                        logger.warning(
                            f"Failed to update all nulls in {table_name}.{timestamp_column}"
                        )
                else:
                    print(f"   → Would update {null_count} rows")
                    total_updated += null_count

            except Exception as e:
                logger.error(f"Error processing {table_name}.{timestamp_column}: {e}")
                print(f"   ✗ Error: {e}")
                continue

    print("=" * 70)
    if dry_run:
        print(f"\nDRY RUN: Would update {total_updated} total rows")
    else:
        print(f"\n✓ Successfully updated {total_updated} total rows")
    logger.info(f"Timestamp fix completed. Total updated: {total_updated}")

    engine.dispose()
    return total_updated

if __name__ == "__main__":
    # Check for --dry-run flag
    dry_run = "--dry-run" in sys.argv

    if dry_run:
        print("\n🔍 Running in DRY RUN mode - no changes will be made\n")

    try:
        updated = fix_null_timestamps(dry_run=dry_run)

        if dry_run:
            print("\nTo apply these changes, run without --dry-run flag:")
            print("  python fix_null_timestamps.py")

        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"\n✗ Fatal error: {e}")
        sys.exit(1)
