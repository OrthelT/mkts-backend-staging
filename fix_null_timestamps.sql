-- Fix null timestamps in all tables
-- Run this SQL script to update null timestamp values with current UTC time
--
-- Usage: sqlite3 wcmkt2.db < fix_null_timestamps.sql

-- First, let's see what we're working with
SELECT 'Checking for null timestamps...' as status;

SELECT 'doctrines' as table_name, COUNT(*) as null_count
FROM doctrines WHERE timestamp IS NULL;

SELECT 'marketorders' as table_name, COUNT(*) as null_count
FROM marketorders WHERE issued IS NULL;

SELECT 'market_history' as table_name, COUNT(*) as null_count
FROM market_history WHERE timestamp IS NULL;

SELECT 'marketstats' as table_name, COUNT(*) as null_count
FROM marketstats WHERE last_update IS NULL;

SELECT 'region_orders' as table_name, COUNT(*) as null_count
FROM region_orders WHERE timestamp IS NULL;

SELECT 'region_history' as table_name, COUNT(*) as null_count
FROM region_history WHERE timestamp IS NULL;

SELECT 'jita_history' as table_name, COUNT(*) as null_count
FROM jita_history WHERE timestamp IS NULL;

SELECT 'updatelog' as table_name, COUNT(*) as null_count
FROM updatelog WHERE timestamp IS NULL;

-- Now perform the updates
SELECT 'Updating null timestamps...' as status;

-- Update doctrines table
UPDATE doctrines
SET timestamp = datetime('now')
WHERE timestamp IS NULL;

-- Update marketorders table
UPDATE marketorders
SET issued = datetime('now')
WHERE issued IS NULL;

-- Update market_history table
UPDATE market_history
SET timestamp = datetime('now')
WHERE timestamp IS NULL;

-- Update marketstats table
UPDATE marketstats
SET last_update = datetime('now')
WHERE last_update IS NULL;

-- Update region_orders table
UPDATE region_orders
SET timestamp = datetime('now')
WHERE timestamp IS NULL;

-- Update region_history table
UPDATE region_history
SET timestamp = datetime('now')
WHERE timestamp IS NULL;

-- Update jita_history table
UPDATE jita_history
SET timestamp = datetime('now')
WHERE timestamp IS NULL;

-- Update updatelog table
UPDATE updatelog
SET timestamp = datetime('now')
WHERE timestamp IS NULL;

-- Verify all updates
SELECT 'Verification - remaining null timestamps:' as status;

SELECT 'doctrines' as table_name, COUNT(*) as remaining_nulls
FROM doctrines WHERE timestamp IS NULL;

SELECT 'marketorders' as table_name, COUNT(*) as remaining_nulls
FROM marketorders WHERE issued IS NULL;

SELECT 'market_history' as table_name, COUNT(*) as remaining_nulls
FROM market_history WHERE timestamp IS NULL;

SELECT 'marketstats' as table_name, COUNT(*) as remaining_nulls
FROM marketstats WHERE last_update IS NULL;

SELECT 'region_orders' as table_name, COUNT(*) as remaining_nulls
FROM region_orders WHERE timestamp IS NULL;

SELECT 'region_history' as table_name, COUNT(*) as remaining_nulls
FROM region_history WHERE timestamp IS NULL;

SELECT 'jita_history' as table_name, COUNT(*) as remaining_nulls
FROM jita_history WHERE timestamp IS NULL;

SELECT 'updatelog' as table_name, COUNT(*) as remaining_nulls
FROM updatelog WHERE timestamp IS NULL;

SELECT 'Done!' as status;
