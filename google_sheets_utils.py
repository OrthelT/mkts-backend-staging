import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from proj_config import google_private_key_file, google_sheet_url, sheet_name
from logging_config import configure_logging
import re
import json
logger = configure_logging(__name__)

# Google Sheets API scopes
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

sheets_dict_file = "sheets_dict.json"

def get_google_sheets_client():
    """
    Initialize and return Google Sheets client using service account credentials
    """
    try:
        credentials = Credentials.from_service_account_file(
            google_private_key_file, scopes=SCOPES
        )
        client = gspread.authorize(credentials)
        logger.info("Google Sheets client initialized successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Google Sheets client: {e}")
        raise

def extract_sheet_id_from_url(url: str) -> str:
    """
    Extract the spreadsheet ID from a Google Sheets URL
    """
    # Pattern to match spreadsheet ID from various URL formats
    pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
    match = re.search(pattern, url)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"Could not extract spreadsheet ID from URL: {url}")

def update_google_sheet(data: pd.DataFrame, append_data: bool = False):
    """
    Update Google Sheet with data from process_system_orders()
    
    Args:
        data: DataFrame containing market data from process_system_orders()
        append_data: Whether to append data to existing table (False = clear and replace data from A2)
    """
    try:
        # Initialize Google Sheets client
        client = get_google_sheets_client()
        
        # Extract spreadsheet ID from URL
        sheet_id = extract_sheet_id_from_url(google_sheet_url)
        
        # Open the spreadsheet
        spreadsheet = client.open_by_key(sheet_id)
        
        # Get or create the worksheet
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            logger.info(f"Found existing worksheet: {sheet_name}")
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
            logger.info(f"Created new worksheet: {sheet_name}")
        
        # Convert DataFrame to list of lists for Google Sheets API
        # Only include data rows (no headers)
        values = data.values.tolist()
        
        if append_data:
            # Find the next empty row starting from A2
            # Get all values to find the last row with data
            try:
                existing_values = worksheet.get_all_values()
                # Find the first empty row after row 1 (headers)
                next_row = len(existing_values) + 1 if len(existing_values) > 1 else 2
            except:
                # If sheet is empty or error, start at row 2
                next_row = 2
            
            # Update starting from the next empty row
            if values:  # Only update if we have data
                range_name = f'A{next_row}'
                worksheet.update(range_name, values, value_input_option='USER_ENTERED')
                logger.info(f"Appended {len(values)} rows starting at row {next_row}")
        else:
            # Clear existing data (except headers) and insert new data
            # Clear from A2 downwards - use a large range to ensure all data is cleared
            worksheet.batch_clear(['A2:Z10000'])
            if values:  # Only update if we have data
                worksheet.update('A2', values, value_input_option='USER_ENTERED')
                logger.info(f"Cleared existing data and inserted {len(values)} rows starting at A2")
            else:
                logger.info("Cleared existing data, no new data to insert")
        
        logger.info(f"Successfully updated Google Sheet with {len(data)} rows of data")
        print(f"Google Sheet updated successfully with {len(data)} rows")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to update Google Sheet: {e}")
        print(f"Error updating Google Sheet: {e}")
        return False

def update_sheet_with_system_orders(system_id: int):
    """
    Convenience function to process system orders and update Google Sheet
    
    Args:
        system_id: System ID to process orders for
    """
    try:
        # Import here to avoid circular imports
        from nakah import process_system_orders
        
        # Get the processed system orders data
        data = process_system_orders(system_id)
        
        if data is not None and not data.empty:
            # Update the Google Sheet
            success = update_google_sheet(data)
            return success
        else:
            logger.warning("No data returned from process_system_orders")
            return False
            
    except Exception as e:
        logger.error(f"Failed to update sheet with system orders: {e}")
        return False

def get_all_worksheets(sheet_name: str) -> list[gspread.Worksheet]:
    """
    Get all worksheets from a Google Sheet
    """
    client = get_google_sheets_client()
    spreadsheet = client.open_by_key(extract_sheet_id_from_url(google_sheet_url))
    return spreadsheet.worksheets()

def get_spreadsheet() -> gspread.Spreadsheet:
    """
    Get a spreadsheet from a Google Sheet
    """
    client = get_google_sheets_client()
    return client.open_by_key(extract_sheet_id_from_url(google_sheet_url))

def get_sheet_dict() -> dict:
    with open(sheets_dict_file, "r") as f:
        return json.load(f)

if __name__ == "__main__":
    pass