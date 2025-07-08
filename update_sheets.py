#!/usr/bin/env python3
"""
Standalone script to update Google Sheets with system orders data
"""

from nakah import process_system_orders
from google_sheets_utils import update_google_sheet
from logging_config import configure_logging

logger = configure_logging(__name__)

def update_sheets_with_system_orders(system_id: int = 30000072):
    """
    Update Google Sheets with system orders data
    
    Args:
        system_id: System ID to process orders for (default: 30000072)
    """
    try:
        logger.info(f"Processing system orders for system ID: {system_id}")
        
        # Get the processed system orders data
        system_orders = process_system_orders(system_id)
        
        if system_orders is not None and not system_orders.empty:
            # Update the Google Sheet
            success = update_google_sheet(system_orders)
            if success:
                logger.info("Google Sheets update completed successfully")
                print("Google Sheets update completed successfully")
            else:
                logger.error("Google Sheets update failed")
                print("Google Sheets update failed")
            return success
        else:
            logger.warning("No data returned from process_system_orders")
            print("No data to update in Google Sheets")
            return False
            
    except Exception as e:
        logger.error(f"Failed to update Google Sheets: {e}")
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    # Run the update
    update_sheets_with_system_orders()