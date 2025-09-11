#!/usr/bin/env python3
"""
Example usage of the refactored GoogleSheetConfig class.
This demonstrates how to use the new integrated functionality.
"""

import sys
import os
import pandas as pd

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.gsheets_config import GoogleSheetConfig

def main():
    """Demonstrate various ways to use the GoogleSheetConfig class."""

    # Example 1: Basic usage with default configuration
    print("=== Example 1: Basic Usage ===")
    gs_config = GoogleSheetConfig()

    # Create some sample data
    sample_data = pd.DataFrame({
        'Item': ['Item 1', 'Item 2', 'Item 3'],
        'Price': [100, 200, 300],
        'Quantity': [10, 20, 30]
    })

    print(f"Sample data:\n{sample_data}")

    # Update the sheet (this would actually update Google Sheets if credentials are valid)
    # success = gs_config.update_sheet(sample_data)
    # print(f"Update successful: {success}")

    # Example 2: Custom configuration
    print("\n=== Example 2: Custom Configuration ===")
    custom_gs = GoogleSheetConfig(
        sheet_name="Custom Sheet",
        private_key_file="path/to/custom/credentials.json"
    )

    print(f"Custom sheet name: {custom_gs.sheet_name}")
    print(f"Custom credentials file: {custom_gs.google_private_key_file}")

    # Example 3: Working with different worksheets
    print("\n=== Example 3: Multiple Worksheets ===")

    # Get all worksheets
    try:
        worksheets = gs_config.get_all_worksheets()
        print(f"Found {len(worksheets)} worksheets")
        for ws in worksheets:
            print(f"  - {ws.title}")
    except Exception as e:
        print(f"Error getting worksheets: {e}")

    # Example 4: Reading data from a worksheet
    print("\n=== Example 4: Reading Data ===")
    try:
        # This would read actual data if the sheet exists
        # data = gs_config.get_worksheet_data("Market Data")
        # print(f"Read {len(data)} rows of data")
        print("Reading data would work if sheet exists and credentials are valid")
    except Exception as e:
        print(f"Error reading data: {e}")

    # Example 5: Appending data
    print("\n=== Example 5: Appending Data ===")
    append_data = pd.DataFrame({
        'Item': ['Item 4', 'Item 5'],
        'Price': [400, 500],
        'Quantity': [40, 50]
    })

    print(f"Data to append:\n{append_data}")
    # success = gs_config.update_sheet(append_data, append_data=True)
    # print(f"Append successful: {success}")

    # Example 6: Using the convenience method for system orders
    print("\n=== Example 6: System Orders ===")
    # This would process system orders and update the sheet
    # success = gs_config.update_sheet_with_system_orders(system_id=30000072)
    # print(f"System orders update successful: {success}")
    print("System orders update would work if nakah module is available")

if __name__ == "__main__":
    main()
