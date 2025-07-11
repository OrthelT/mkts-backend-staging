# Eve Online Market Data Collection & Analysis System

A comprehensive market data collection and analysis system for Eve Online that fetches market data from the ESI API, processes it, and provides insights for market analysis and fleet doctrine planning.

## Features

- **Market Data Collection**: Fetches real-time market orders from Eve Online's ESI API
- **Historical Analysis**: Collects and analyzes market history for trend analysis
- **Doctrine Analysis**: Calculates ship fitting availability and market depth
- **Regional Processing**: Handles both structure-specific and region-wide market data
- **Google Sheets Integration**: Automatically updates spreadsheets with market data
- **Market Value Calculation**: Calculates total market value excluding blueprints/skills
- **Ship Count Tracking**: Tracks ship availability on the market
- **Multi-Database Support**: Local SQLite with optional remote Turso sync

## Quick Start

### Prerequisites

- Python 3.12+
- Eve Online Developer Application (for ESI API access)
- Google Service Account (for Sheets integration)

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd mkts_backend
```

2. Install dependencies using uv:
```bash
uv sync
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your credentials
```

### Configuration

Create a `.env` file with the following variables:

```env
CLIENT_ID=<eve_sso_client_id>
SECRET_KEY=<eve_sso_client_secret>
TURSO_URL=<optional_remote_db_url>
TURSO_AUTH_TOKEN=<optional_remote_db_token>
SDE_URL=<optional_sde_db_url>
SDE_AUTH_TOKEN=<optional_sde_db_token>
```

### Running the Application

```bash
# Run with market orders only
python main.py

# Run with historical data processing
python main.py --history
```

## Architecture

### Core Components

- **`main.py`**: Main application orchestrating data collection and processing
- **`dbhandler.py`**: Database operations and ORM session management
- **`models.py`**: SQLAlchemy ORM models for all data tables
- **`nakah.py`**: Regional market processing and system-specific analysis
- **`google_sheets_utils.py`**: Google Sheets integration and automation
- **`data_processing.py`**: Market statistics and doctrine analysis calculations
- **`ESI_OAUTH_FLOW.py`**: Eve Online SSO authentication handling

### Data Flow

1. **Authentication**: Authenticate with Eve SSO using required scopes
2. **Market Orders**: Fetch current market orders for configured structure
3. **Historical Data**: Optionally fetch historical data for watchlist items
4. **Statistics**: Calculate market statistics (price, volume, days remaining)
5. **Doctrine Analysis**: Analyze ship fitting availability based on market data
6. **Regional Processing**: Update regional orders for deployment region
7. **System Analysis**: Process system-specific orders and calculate market metrics
8. **Google Sheets**: Update spreadsheets with system market data
9. **Storage**: Store all results in local database with optional cloud sync

## Configuration

### Key Settings

- **Structure ID**: `1035466617946` (4-HWWF Keepstar)
- **Region ID**: `10000003` (The Vale of Silent)
- **Deployment Region**: `10000001` (The Forge)
- **Deployment System**: `30000072` (Nakah)
- **Database**: Local SQLite (`wcmkt2.db`) with optional Turso sync
- **Watchlist**: CSV-based item tracking in `databackup/all_watchlist.csv`

### Google Sheets Integration

1. Create a Google Service Account
2. Download the service account key file
3. Place the key file as `wcdoctrines-1f629d861c2f.json` in the project root
4. Configure the spreadsheet URL in `proj_config.py`

## Database Schema

### Primary Tables

- **`marketorders`**: Current market orders from ESI API
- **`market_history`**: Historical market data for trend analysis
- **`marketstats`**: Calculated market statistics and metrics
- **`doctrines`**: Ship fitting availability and doctrine analysis
- **`region_orders`**: Regional market orders for broader analysis
- **`watchlist`**: Items being tracked for market analysis

### Support Tables

- **`ship_targets`**: Ship production targets and goals
- **`doctrine_map`**: Mapping between doctrines and fittings
- **`doctrine_info`**: Doctrine metadata and information

## API Integration

### Eve Online ESI API

- **Market Orders**: Real-time market data from structures
- **Market History**: Historical price and volume data
- **Universe Names**: Item name resolution
- **OAuth Flow**: Secure authentication for protected endpoints

### Google Sheets API

- **Service Account**: Authentication using service account credentials
- **Batch Updates**: Efficient bulk data updates
- **Configurable Modes**: Append or replace data options

## Development

### Dependencies

The project uses modern Python dependencies managed with uv:

- **SQLAlchemy**: ORM and database operations
- **Pandas**: Data manipulation and analysis
- **Requests**: HTTP client for API calls
- **libsql**: SQLite with sync capabilities
- **gspread**: Google Sheets API integration
- **mydbtools**: Custom database utilities

### Testing

```bash
# Run tests
python -m pytest tests/
```

### Logging

Comprehensive logging is configured with rotating file handlers:

- **Log Files**: `logs/mkts-backend.log`
- **Rotation**: 1MB per file, 5 backup files
- **Levels**: INFO for file, ERROR for console

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is developed as a learning project for Eve Online market analysis. Contact orthel_toralen on Discord with questions.

## Disclaimer

This tool is designed for educational and analysis purposes. All Eve Online data is provided by CCP Games through their ESI API. Eve Online is a trademark of CCP Games.