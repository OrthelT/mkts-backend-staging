import os
import json
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import re
from typing import Optional, List
from mkts_backend.config.logging_config import configure_logging
from mkts_backend.config.config import load_settings
from dotenv import load_dotenv

load_dotenv()
logger = configure_logging(__name__)

settings = load_settings()

"""
This module configures the Google Sheets API and updates a spreadsheet with market data.
"""

class GoogleSheetConfig:
    _google_private_key_file = os.getenv("GOOGLE_SHEETS_PRIVATE_KEY")
    _google_sheet_url = settings["google_sheets"]["sheet_url"]
    _default_sheet_name = "market_data"
    _default_clear_range = "A2:Z10000"
    _default_worksheet_rows = 1000
    _default_worksheet_cols = 20
    _enabled = settings["google_sheets"]["enabled"]

    _scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    def __init__(
        self,
        private_key_file: Optional[str] = None,
        sheet_url: Optional[str] = None,
        sheet_name: Optional[str] = None,
    ):
        if not self._enabled:
            logger.info("Google Sheets integration is disabled in settings")
            return
        self.google_private_key_file = private_key_file or self._google_private_key_file
        self.google_sheet_url = sheet_url or self._google_sheet_url
        self.sheet_name = sheet_name or self._default_sheet_name
        self._client = None
        self._spreadsheet = None

    def get_client(self) -> gspread.Client:
        if self._client is None:
            try:
                google_credentials_json = os.getenv("GOOGLE_SHEET_KEY")

                if google_credentials_json:
                    try:
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
        target_url = url or self.google_sheet_url
        pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
        match = re.search(pattern, target_url)
        if match:
            return match.group(1)
        else:
            raise ValueError(f"Could not extract spreadsheet ID from URL: {target_url}")

    def get_spreadsheet(self, sheet_url: Optional[str] = None) -> gspread.Spreadsheet:
        if self._spreadsheet is None or sheet_url:
            client = self.get_client()
            sheet_id = self.extract_sheet_id_from_url(sheet_url)
            self._spreadsheet = client.open_by_key(sheet_id)
        return self._spreadsheet

    def get_worksheet(self, sheet_name: Optional[str] = None, create_if_missing: bool = True) -> gspread.Worksheet:
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
                    cols=self._default_worksheet_cols,
                )
                logger.info(f"Created new worksheet: {target_sheet_name}")
                return worksheet
            else:
                raise

    def get_all_worksheets(self, sheet_url: Optional[str] = None) -> List[gspread.Worksheet]:
        spreadsheet = self.get_spreadsheet(sheet_url)
        return spreadsheet.worksheets()

    def update_sheet(
        self,
        data: pd.DataFrame,
        sheet_name: Optional[str] = None,
        append_data: bool = False,
        clear_range: Optional[str] = None,
    ) -> bool:
        try:
            worksheet = self.get_worksheet(sheet_name)

            data = data.infer_objects()
            data = data.fillna(0)
            data = data.reset_index(drop=True)
            logger.info(f"Data shape: {data.shape}")
            logger.info(f"Data columns: {list(data.columns)}")

            values = data.values.tolist()

            if append_data:
                try:
                    existing_values = worksheet.get_all_values()
                    next_row = len(existing_values) + 1 if len(existing_values) > 1 else 2
                except:
                    next_row = 2

                if values:
                    range_name = f'A{next_row}'
                    worksheet.update(range_name, values, value_input_option='USER_ENTERED')
                    logger.info(f"Appended {len(values)} rows starting at row {next_row}")
            else:
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
