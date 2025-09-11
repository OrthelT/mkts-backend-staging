import sys
import os
import json
# Add the project root to Python path for direct execution
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import re
from typing import Optional, List
from config.logging_config import configure_logging

logger = configure_logging(__name__)
"""
This module is used to configure the Google Sheets API and update a spreadsheet with market data.
"""

class GoogleSheetConfig:
    # Private class variables for configuration
    _google_private_key_file = "wcupdates-6909ae0dfa86.json"  # Fallback file path
    _google_sheet_url = "https://docs.google.com/spreadsheets/d/1RmNJB9Yz4lG6kKKitGQ0zDuPbOiSe0ywn4SbSKabdwc/edit?gid=0#gid=0"
    _default_sheet_name = "market_data"
    _default_clear_range = "A2:Z10000"
    _default_worksheet_rows = 1000
    _default_worksheet_cols = 20

    # Google Sheets API scopes
    _scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    def __init__(self,
                 private_key_file: Optional[str] = None,
                 sheet_url: Optional[str] = None,
                 sheet_name: Optional[str] = None,
                ):
        """
        Initialize GoogleSheetConfig with optional parameters.

        Args:
            private_key_file: Path to Google service account JSON file
            sheet_url: Google Sheets URL
            sheet_name: Default worksheet name
        """
        self.google_private_key_file = private_key_file or self._google_private_key_file
        self.google_sheet_url = sheet_url or self._google_sheet_url
        self.sheet_name = sheet_name or self._default_sheet_name
        self._client = None
        self._spreadsheet = None


    def get_client(self) -> gspread.Client:
        """
        Initialize and return Google Sheets client using service account credentials.
        Uses caching to avoid re-initialization.
        """
        if self._client is None:
            try:
                # Try to get credentials from environment variable first
                google_credentials_json = os.getenv("GOOGLE_SHEET_KEY")

                if google_credentials_json:
                    try:
                        # Parse JSON from environment variable
                        credentials_info = json.loads(google_credentials_json)
                        credentials = Credentials.from_service_account_info(
                            credentials_info, scopes=self._scopes
                        )
                        logger.info("Google Sheets client initialized from environment variable")
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse GOOGLE_SHEET_KEY JSON: {e}")
                        logger.info("Falling back to file-based credentials")
                        credentials = Credentials.from_service_account_file(
                            self.google_private_key_file, scopes=self._scopes
                        )
                        logger.info("Google Sheets client initialized from file")
                else:
                    # Fallback to file-based credentials
                    credentials = Credentials.from_service_account_file(
                        self.google_private_key_file, scopes=self._scopes
                    )
                    logger.info("Google Sheets client initialized from file")

                self._client = gspread.authorize(credentials)
                logger.info("Google Sheets client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Google Sheets client: {e}")
                raise
        return self._client

    def extract_sheet_id_from_url(self, url: Optional[str] = None) -> str:
        """
        Extract the spreadsheet ID from a Google Sheets URL.

        Args:
            url: Google Sheets URL (uses instance URL if not provided)

        Returns:
            Spreadsheet ID string
        """
        target_url = url or self.google_sheet_url
        pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
        match = re.search(pattern, target_url)
        if match:
            return match.group(1)
        else:
            raise ValueError(f"Could not extract spreadsheet ID from URL: {target_url}")

    def get_spreadsheet(self, sheet_url: Optional[str] = None) -> gspread.Spreadsheet:
        """
        Get a spreadsheet object.

        Args:
            sheet_url: Google Sheets URL (uses instance URL if not provided)

        Returns:
            Spreadsheet object
        """
        if self._spreadsheet is None or sheet_url:
            client = self.get_client()
            sheet_id = self.extract_sheet_id_from_url(sheet_url)
            self._spreadsheet = client.open_by_key(sheet_id)
        return self._spreadsheet

    def get_worksheet(self, sheet_name: Optional[str] = None,
                     create_if_missing: bool = True) -> gspread.Worksheet:
        """
        Get a worksheet from the spreadsheet.

        Args:
            sheet_name: Worksheet name (uses instance sheet_name if not provided)
            create_if_missing: Whether to create worksheet if it doesn't exist

        Returns:
            Worksheet object
        """
        target_sheet_name = sheet_name or self.sheet_name
        spreadsheet = self.get_spreadsheet()

        try:
            worksheet = spreadsheet.worksheet(target_sheet_name)
            logger.info(f"Found existing worksheet: {target_sheet_name}")
            return worksheet
        except gspread.WorksheetNotFound:
            if create_if_missing:
                worksheet = spreadsheet.add_worksheet(
                    title=target_sheet_name,
                    rows=self._default_worksheet_rows,
                    cols=self._default_worksheet_cols
                )
                logger.info(f"Created new worksheet: {target_sheet_name}")
                return worksheet
            else:
                raise

    def get_all_worksheets(self, sheet_url: Optional[str] = None) -> List[gspread.Worksheet]:
        """
        Get all worksheets from a Google Sheet.

        Args:
            sheet_url: Google Sheets URL (uses instance URL if not provided)

        Returns:
            List of worksheet objects
        """
        spreadsheet = self.get_spreadsheet(sheet_url)
        return spreadsheet.worksheets()

    def update_sheet(self, data: pd.DataFrame,
                    sheet_name: Optional[str] = None,
                    append_data: bool = False,
                    clear_range: Optional[str] = None) -> bool:
        """
        Update Google Sheet with DataFrame data.

        Args:
            data: DataFrame containing data to update
            sheet_name: Worksheet name (uses instance sheet_name if not provided)
            append_data: Whether to append data (False = clear and replace from A2)
            clear_range: Range to clear when not appending (uses default if not provided)

        Returns:
            True if successful, False otherwise
        """
        try:
            worksheet = self.get_worksheet(sheet_name)

            # Clean data for Google Sheets
            data = data.infer_objects()
            data = data.fillna(0)
            data = data.reset_index(drop=True)
            logger.info(f"Data shape: {data.shape}")
            logger.info(f"Data columns: {list(data.columns)}")

            # Convert DataFrame to list of lists for Google Sheets API
            values = data.values.tolist()

            if append_data:
                # Find the next empty row starting from A2
                try:
                    existing_values = worksheet.get_all_values()
                    next_row = len(existing_values) + 1 if len(existing_values) > 1 else 2
                except:
                    next_row = 2

                # Update starting from the next empty row
                if values:
                    range_name = f'A{next_row}'
                    worksheet.update(range_name, values, value_input_option='USER_ENTERED')
                    logger.info(f"Appended {len(values)} rows starting at row {next_row}")
            else:
                # Clear existing data and insert new data
                clear_target = clear_range or self._default_clear_range
                worksheet.batch_clear([clear_target])

                if values:
                    worksheet.update('A2', values, value_input_option='USER_ENTERED')
                    logger.info(f"Cleared existing data and inserted {len(values)} rows starting at A2")
                else:
                    logger.info("Cleared existing data, no new data to insert")

            logger.info(f"Successfully updated Google Sheet with {len(data)} rows of data")
            return True

        except Exception as e:
            logger.error(f"Failed to update Google Sheet: {e}")
            return False

    def update_sheet_with_system_orders(self, system_id: int,
                                      sheet_name: Optional[str] = None) -> bool:
        """
        Convenience method to process system orders and update Google Sheet.

        Args:
            system_id: System ID to process orders for
            sheet_name: Worksheet name (uses instance sheet_name if not provided)

        Returns:
            True if successful, False otherwise
        """
        try:
            # Import here to avoid circular imports
            from utils.nakah import process_system_orders

            # Get the processed system orders data
            data = process_system_orders(system_id)

            if data is not None and not data.empty:
                # Update the Google Sheet
                success = self.update_sheet(data, sheet_name)
                return success
            else:
                logger.warning("No data returned from process_system_orders")
                return False

        except Exception as e:
            logger.error(f"Failed to update sheet with system orders: {e}")
            return False

    def clear_worksheet(self, sheet_name: Optional[str] = None,
                       clear_range: Optional[str] = None) -> bool:
        """
        Clear data from a worksheet.

        Args:
            sheet_name: Worksheet name (uses instance sheet_name if not provided)
            clear_range: Range to clear (uses default if not provided)

        Returns:
            True if successful, False otherwise
        """
        try:
            worksheet = self.get_worksheet(sheet_name, create_if_missing=False)
            clear_target = clear_range or self._default_clear_range
            worksheet.batch_clear([clear_target])
            logger.info(f"Cleared range {clear_target} from worksheet {sheet_name or self.sheet_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to clear worksheet: {e}")
            return False

    def get_worksheet_data(self, sheet_name: Optional[str] = None,
                          as_dataframe: bool = True) -> pd.DataFrame:
        """
        Get all data from a worksheet.

        Args:
            sheet_name: Worksheet name (uses instance sheet_name if not provided)
            as_dataframe: Whether to return as DataFrame (True) or list of lists (False)

        Returns:
            DataFrame or list of lists containing worksheet data
        """
        try:
            worksheet = self.get_worksheet(sheet_name, create_if_missing=False)
            data = worksheet.get_all_values()

            if as_dataframe and data:
                # Use first row as headers
                headers = data[0]
                rows = data[1:] if len(data) > 1 else []
                return pd.DataFrame(rows, columns=headers)
            else:
                return data

        except Exception as e:
            logger.error(f"Failed to get worksheet data: {e}")
            return pd.DataFrame() if as_dataframe else []

if __name__ == "__main__":
    sheet = GoogleSheetConfig()
    data = sheet.get_worksheet_data()
    print(data)