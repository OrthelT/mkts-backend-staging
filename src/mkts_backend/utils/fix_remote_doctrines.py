#!/usr/bin/env python3
"""
Script to fix the remote doctrines table schema to support auto-increment for the id column.
This addresses the SQLAlchemy FlushError about NULL identity keys.

Usage:
    python fix_remote_doctrines.py

This script will:
1. Create a new doctrines table with proper auto-increment on the id column
2. Migrate all existing data from the old table to the new one
3. Replace the old table with the new one
4. Verify the fix was successful
"""

from mkts_backend.config.config import DatabaseConfig
from sqlalchemy import text
from mkts_backend.config.logging_config import configure_logging

logger = configure_logging(__name__)


def fix_remote_doctrines_table():
    """Fix the remote doctrines table to have proper auto-increment on the id column."""

    # Use remote engine
    mkt_db = DatabaseConfig('wcmkt')
    engine = mkt_db.remote_engine

    try:
        with engine.connect() as conn:
            # Start a transaction
            trans = conn.begin()

            try:
                logger.info("Starting remote doctrines table schema fix...")

                # Step 1: Create new table with proper auto-increment
                logger.info("Creating new doctrines table with auto-increment...")
                create_sql = """
                CREATE TABLE doctrines_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fit_id INTEGER,
                    ship_id INTEGER,
                    ship_name TEXT,
                    hulls INTEGER,
                    type_id INTEGER,
                    type_name TEXT,
                    fit_qty INTEGER,
                    fits_on_mkt INTEGER,
                    total_stock INTEGER,
                    price FLOAT,
                    avg_vol INTEGER,
                    days FLOAT,
                    group_id INTEGER,
                    group_name TEXT,
                    category_id INTEGER,
                    category_name TEXT,
                    timestamp TEXT
                )
                """
                conn.execute(text(create_sql))
                logger.info("New table created successfully")

                # Step 2: Copy all existing data
                logger.info("Copying existing data...")
                result = conn.execute(text("SELECT COUNT(*) FROM doctrines"))
                count = result.scalar()
                logger.info(f"Found {count} existing records to migrate")

                if count > 0:
                    conn.execute(text("INSERT INTO doctrines_new SELECT * FROM doctrines"))
                    logger.info("Data migration completed")
                else:
                    logger.info("No existing data to migrate")

                # Step 3: Drop old table
                logger.info("Dropping old doctrines table...")
                conn.execute(text("DROP TABLE doctrines"))

                # Step 4: Rename new table
                logger.info("Renaming new table...")
                conn.execute(text("ALTER TABLE doctrines_new RENAME TO doctrines"))

                # Commit the transaction
                trans.commit()
                logger.info("Remote doctrines table schema fix completed successfully!")

                # Verify the fix
                result = conn.execute(text("PRAGMA table_info(doctrines)"))
                for row in result:
                    if row.name == 'id':
                        logger.info(f"ID column info: name={row.name}, type={row.type}, pk={row.pk}")
                        if row.pk == 1:
                            logger.info("✅ Auto-increment is now properly configured!")
                        else:
                            logger.error("❌ Auto-increment configuration failed!")

                # Verify data integrity
                result = conn.execute(text("SELECT COUNT(*) FROM doctrines"))
                final_count = result.scalar()
                logger.info(f"Final record count: {final_count}")

                if count > 0 and final_count != count:
                    logger.error(f"⚠️  Data integrity issue: Expected {count} records, found {final_count}")
                else:
                    logger.info("✅ Data integrity verified")

            except Exception as e:
                trans.rollback()
                logger.error(f"Error during schema fix, rolling back: {e}")
                raise

    except Exception as e:
        logger.error(f"Failed to fix remote doctrines table: {e}")
        raise


def check_remote_doctrines_schema():
    """Check the current schema of the remote doctrines table."""
    mkt_db = DatabaseConfig('wcmkt')
    engine = mkt_db.remote_engine

    logger.info("Checking remote doctrines table schema...")

    try:
        with engine.connect() as conn:
            result = conn.execute(text("PRAGMA table_info(doctrines)"))
            logger.info("Current doctrines table schema:")
            for row in result:
                logger.info(f"  {row.cid}|{row.name}|{row.type}|{row.notnull}|{row.dflt_value}|{row.pk}")

            # Check if auto-increment is properly configured
            result = conn.execute(text("PRAGMA table_info(doctrines)"))
            id_column = next((row for row in result if row.name == 'id'), None)

            if id_column:
                if id_column.pk == 1 and 'AUTOINCREMENT' in str(id_column.type).upper():
                    logger.info("✅ Auto-increment is properly configured")
                    return True
                else:
                    logger.warning("❌ Auto-increment is NOT properly configured")
                    return False
            else:
                logger.error("❌ No id column found")
                return False

    except Exception as e:
        logger.error(f"Error checking schema: {e}")
        return False


def main():
    """Main function to run the schema fix."""
    logger.info("=" * 80)
    logger.info("Remote Doctrines Table Schema Fix")
    logger.info("=" * 80)

    # First check current schema
    logger.info("Checking current schema...")
    schema_ok = check_remote_doctrines_schema()

    if schema_ok:
        logger.info("Schema is already correct. No fix needed.")
        return

    # Ask for confirmation
    logger.warning("Schema needs to be fixed. This will:")
    logger.warning("1. Create a new table with proper auto-increment")
    logger.warning("2. Migrate all existing data")
    logger.warning("3. Replace the old table")
    logger.warning("4. This operation is irreversible!")

    try:
        response = input("\nDo you want to proceed? (yes/no): ").lower().strip()
        if response not in ['yes', 'y']:
            logger.info("Operation cancelled by user.")
            return
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user.")
        return

    # Perform the fix
    try:
        fix_remote_doctrines_table()
        logger.info("=" * 80)
        logger.info("✅ Schema fix completed successfully!")
        logger.info("=" * 80)
    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"❌ Schema fix failed: {e}")
        logger.error("=" * 80)
        raise


if __name__ == "__main__":
    main()
