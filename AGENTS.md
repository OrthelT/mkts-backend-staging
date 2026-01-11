# LLM Agent Guide: Eve Online Market Data System

This guide provides comprehensive documentation for LLM agents working with this Eve Online Market Data Collection and Analysis System. It covers both assisting users in implementing their own system and working with the existing codebase.

## Quick Start for Development

**Run the main application:**
```bash
uv run mkts-backend
```

**Include historical data:**
```bash
uv run mkts-backend --history
```

**Check database tables:**
```bash
uv run mkts-backend --check_tables
```

**Dependencies are managed with uv:**
```bash
uv sync  # Install dependencies
uv add <package>  # Add new dependency
```

## System Overview

This is a comprehensive Eve Online market data collection and analysis system consisting of two repositories:

1. **mkts_backend** (this repo): Backend data collection, processing, and storage
   - Fetches market data from Eve Online ESI API for specific structures/regions
   - Processes and stores market orders, history, and calculated statistics in SQLite databases
   - Analyzes doctrine fits and calculates market availability for ship loadouts
   - Tracks regional/system market data with automated Google Sheets integration
   - Supports local and remote (Turso) database sync

2. **wcmkts_new** (frontend): Streamlit web application for data visualization
   - Repository: https://github.com/OrthelT/wcmkts_new
   - Displays market statistics and trends
   - Shows doctrine/fitting availability
   - Provides interactive data exploration

## Core Components and Architecture

### Main Data Flow (`cli.py`)
The primary orchestration file that coordinates all data collection and processing:
- `fetch_market_orders()` - Gets current market orders from ESI API with OAuth
- `fetch_history()` - Gets historical market data for watchlist items from primary region
- `fetch_jita_history()` - Gets comparative historical data from The Forge region (Jita)
- `calculate_market_stats()` - Computes statistics from orders and history
- `calculate_doctrine_stats()` - Analyzes ship fitting availability
- Regional order processing and system-specific market analysis

### Database Layer (`dbhandler.py`)
Manages all database operations:
- Handles both local SQLite and remote Turso database sync
- Functions for CRUD operations on market data tables
- Database sync functionality for production deployment
- ORM-based data insertion with chunking for large datasets

### Data Models (`models.py`)
SQLAlchemy ORM model definitions:
- **Core Models:** `MarketOrders`, `MarketHistory`, `MarketStats`, `Doctrines`, `Watchlist`
- **Regional Models:** `RegionOrders`, `JitaHistory` (comparative pricing from The Forge)
- **Organizational Models:** `ShipTargets`, `DoctrineMap`, `DoctrineInfo`
- All tables use primary database `wcmkt2.db`

### OAuth Authentication (`ESI_OAUTH_FLOW.py` / `esi_auth.py`)
Handles Eve Online SSO authentication:
- Eve Online SSO authentication for ESI API access
- Token refresh and storage in `token.json`
- Manages OAuth flow for initial authorization

### Regional Market Processing (`nakah.py`)
Specialized regional market data handling:
- `get_region_orders()` - Fetches all market orders for a region
- `process_system_orders()` - Processes orders for specific systems
- `calculate_total_market_value()` - Calculates total market value excluding blueprints/skills
- `calculate_total_ship_count()` - Counts ships available on the market

### Google Sheets Integration (`google_sheets_utils.py` / `gsheets_config.py`)
Automated spreadsheet updates:
- Automated Google Sheets updates with market data
- Service account authentication
- Configurable append/replace data modes

### Data Processing (`data_processing.py`)
Statistics and analysis calculations:
- Market statistics calculation with 5th percentile pricing
- Doctrine availability analysis
- Historical data integration (30-day averages)

## Key Configuration Values

Current system configuration (customizable in `esi_config.py`):

- **Structure ID:** `1035466617946` (4-HWWF Keepstar)
- **Region ID:** `10000003` (The Vale of Silent)
- **Deployment Region:** `10000001` (The Forge)
- **Deployment System:** `30000072` (Nakah)
- **Database:** Local SQLite (`wcmkt2.db`) with optional Turso sync
- **Watchlist:** CSV-based item tracking in `databackup/all_watchlist.csv`

## External Dependencies

- **EVE Static Data Export (SDE):** `sde_info.db` - game item/type information
- **Custom dbtools:** Local dependency at `../../tools/dbtools` for database utilities
- **Turso/libsql:** For remote database synchronization (optional in dev)
- **Google Sheets API:** For automated market data reporting

## Data Processing Flow

The complete data pipeline when running the application:

1. Authenticate with Eve SSO using required scopes
2. Fetch current market orders for configured structure
3. Fetch historical data for watchlist items (optional with `--history` flag)
   - Primary market history (Vale of Silent) → `MarketHistory` table
   - Jita comparative history (The Forge) → `JitaHistory` table
4. Calculate market statistics (price, volume, days remaining)
5. Calculate doctrine/fitting availability based on market data
6. Update regional orders for deployment region
7. Process system-specific orders and calculate market value/ship count
8. Update Google Sheets with system market data
9. Store all results in local database with optional cloud sync

## Environment Variables Required

```env
# Eve Online ESI Credentials (Required)
CLIENT_ID=<eve_sso_client_id>
SECRET_KEY=<eve_sso_client_secret>
REFRESH_TOKEN=<your_refresh_token_here>

# Google Sheets (Optional)
GOOGLE_SHEET_KEY={"type":"service_account"...}  # Entire JSON key file content

# Turso Remote Database (Optional)
TURSO_URL=<optional_remote_db_url>
TURSO_AUTH_TOKEN=<optional_remote_db_token>
SDE_URL=<optional_sde_db_url>
SDE_AUTH_TOKEN=<optional_sde_db_token>
```

## Additional Features

- **Comparative Market Analysis:** Dual-region history tracking (primary market vs Jita) for price comparison charts
- **Market Value Calculation:** Filters out blueprints and skills for accurate market value assessment
- **Ship Count Tracking:** Specifically tracks ship availability on the market
- **Google Sheets Automation:** Automatically updates spreadsheets with latest market data
- **Multi-Region Support:** Handles both structure-specific and region-wide market data
- **Async Processing:** High-performance concurrent API requests with rate limiting and backoff
- **Error Handling:** Comprehensive logging and error recovery for API failures

---

## User Implementation Guide

## Prerequisites Checklist

Before starting, ensure the user has:

- [ ] Python 3.12 or higher
- [ ] Git installed
- [ ] An Eve Online character with market access to their target structure
- [ ] Access to create Eve Online developer applications
- [ ] (Optional) Google account for Sheets integration
- [ ] (Optional) Turso account for remote database hosting

## Implementation Steps

### Step 1: Eve Online ESI Application Setup

Guide the user through creating an ESI application:

1. **Navigate to Eve Developers Portal**:
   - URL: https://developers.eveonline.com/
   - Have user log in with their Eve Online account

2. **Create New Application**:
   - Click "Create New Application"
   - Application Name: Choose descriptive name (e.g., "My Market Data Collector")
   - Description: Brief description of purpose
   - Callback URL: `http://localhost:8000/callback`
   - Required Scopes:
     - `esi-markets.structure_markets.v1` (for structure market access)
   - Connection Type: "Authentication & API Access"

3. **Save Credentials**:
   - Note the Client ID
   - Note the Secret Key
   - These will be needed for `.env` file

4. **Generate Refresh Token**:
   - User needs to authenticate once to get a refresh token
   - This requires running an OAuth flow locally (documented in ESI_OAUTH_FLOW.py)
   - The refresh token allows unattended operation

### Step 2: Google Service Account Setup (Optional)

If user wants Google Sheets integration:

1. **Create Google Cloud Project**:
   - Navigate to: https://console.cloud.google.com/
   - Create new project or select existing
   - Note the project name

2. **Enable APIs**:
   - Enable "Google Sheets API"
   - Enable "Google Drive API"

3. **Create Service Account**:
   - Navigate to: IAM & Admin > Service Accounts
   - Click "Create Service Account"
   - Name: "market-data-sheets" (or similar)
   - Role: Leave as default or "Editor"
   - Click "Done"

4. **Generate Key**:
   - Click on the created service account
   - Go to "Keys" tab
   - Click "Add Key" > "Create New Key"
   - Choose JSON format
   - Download and save the JSON file
   - Rename to something recognizable (e.g., `market-service-account.json`)

5. **Share Spreadsheet**:
   - Create a Google Sheet for market data
   - Share it with the service account email (found in JSON file, looks like `xxx@xxx.iam.gserviceaccount.com`)
   - Give "Editor" permissions

### Step 3: Clone and Setup Backend Repository

```bash
# Clone the repository
git clone https://github.com/OrthelT/mkts_backend.git
cd mkts_backend

# Install dependencies using uv
pip install uv  # if not already installed
uv sync
```

### Step 4: Configure Environment Variables

Create a `.env` file in the repository root:

```env
# Eve Online ESI Credentials (Required)
CLIENT_ID=your_client_id_here
SECRET_KEY=your_secret_key_here
REFRESH_TOKEN=your_refresh_token_here

# Google Sheets (Optional - for automated updates)
GOOGLE_SHEET_KEY={"type":"service_account","project_id":"..."}  # Entire JSON key file content

# Turso Remote Database (Optional - for production deployment)
TURSO_WCMKT2_URL=libsql://your-db.turso.io
TURSO_WCMKT2_TOKEN=your_token_here
TURSO_WCMKT3_URL=libsql://your-dev-db.turso.io
TURSO_WCMKT3_TOKEN=your_dev_token_here
TURSO_FITTING_URL=libsql://your-fitting-db.turso.io
TURSO_FITTING_TOKEN=your_fitting_token_here
TURSO_SDE_URL=libsql://your-sde-db.turso.io
TURSO_SDE_TOKEN=your_sde_token_here
```

**Important Notes**:
- `REFRESH_TOKEN` must be obtained through OAuth flow (see `src/mkts_backend/esi/esi_auth.py`)
- For local-only operation, Turso credentials are optional
- `GOOGLE_SHEET_KEY` can be the entire JSON content or the system will fall back to a file

### Step 5: Customize Market Configuration

Edit `src/mkts_backend/config/esi_config.py` to match user's market:

```python
class ESIConfig:
    # Update these values for your market
    _region_ids = {
        "primary_region_id": 10000003,  # Change to your region ID
        "secondary_region_id": None     # Optional secondary market
    }
    _system_ids = {
        "primary_system_id": 30000240,  # Change to your system ID
        "secondary_system_id": None
    }
    _structure_ids = {
        "primary_structure_id": 1035466617946,  # Change to your structure ID
        "secondary_structure_id": None
    }
    _names = {
        "primary": "Your Structure Name",
        "secondary": "Secondary Market Name"
    }
```

**Finding Your IDs**:
- **Structure ID**: In-game, right-click structure > Copy > Copy Info > paste somewhere > extract ID from `showinfo:` link
- **Region ID**: Use ESI endpoint: `https://esi.evetech.net/latest/universe/regions/` and search
- **System ID**: Use ESI endpoint: `https://esi.evetech.net/latest/search/?categories=solar_system&search=SystemName`

### Step 6: Setup Initial Data

#### 6.1 Create Watchlist

The watchlist defines which items to track. Create or edit `databackup/all_watchlist.csv`:

```csv
type_id,type_name,group_id,group_name,category_id,category_name
34,Tritanium,18,Mineral,4,Material
35,Pyerite,18,Mineral,4,Material
36,Mexallon,18,Mineral,4,Material
```

**Tips for Watchlist Creation**:
- Start with common items (minerals, ships, modules)
- Use Eve's "Show Info" > "Copy Type ID" to get type_ids
- Or use ESI search: `https://esi.evetech.net/latest/search/?categories=inventory_type&search=ItemName`

#### 6.2 Add Fittings (Optional)

If tracking doctrine availability, add ship fittings:

1. Export fittings from Eve Online (in-game: Fitting window > Import/Export > Copy to Clipboard)
2. Place fitting files in a designated folder
3. Use the fitting parser utilities in `src/mkts_backend/utils/parse_fits.py`

### Step 7: Initialize Databases

```bash
# First run will create local SQLite databases
uv run mkts-backend

# This creates:
# - wcmkt2.db (main market database)
# - wcfitting.db (fittings/doctrines)
# - sde_info.db (Eve static data export)
```

**Database Schema**:
- `marketorders`: Current market orders
- `market_history`: Historical price/volume data
- `marketstats`: Calculated statistics
- `doctrines`: Fitting availability analysis
- `watchlist`: Items being tracked

### Step 8: Configure Google Sheets Integration (Optional)

Edit `src/mkts_backend/config/gsheets_config.py`:

```python
class GoogleSheetConfig:
    _google_private_key_file = "your-service-account.json"  # Path to your JSON key file
    _google_sheet_url = "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit"
    _default_sheet_name = "market_data"  # Sheet tab name
```

### Step 9: Run Backend Data Collection

```bash
# Run basic market data collection
uv run mkts-backend

# Run with historical data processing (recommended)
uv run mkts-backend --history

# Check database contents
uv run mkts-backend --check_tables
```

**Schedule Regular Updates**:

Option A - GitHub Actions (recommended for remote deployment):
- Configure secrets in GitHub repository settings
- See `docs/GITHUB_ACTIONS_SETUP.md` for detailed guide
- Workflow file: `.github/workflows/market-data-collection.yml`

Option B - Cron job (for local server):
```bash
# Edit crontab
crontab -e

# Add entry (runs every 4 hours)
0 */4 * * * cd /path/to/mkts_backend && /path/to/uv run mkts-backend --history >> /path/to/logs/cron.log 2>&1
```

### Step 10: Setup Streamlit Frontend

Clone and setup the frontend application:

```bash
# Clone frontend repository
cd ..
git clone https://github.com/OrthelT/wcmkts_new.git
cd wcmkts_new

# Install dependencies
pip install -r requirements.txt
```

**Configure Database Connection**:

The frontend needs access to the backend database. Options:

1. **Local Database** (development):
   - Copy or symlink `wcmkt2.db` from backend to frontend directory
   - Update database path in frontend config

2. **Remote Database** (production):
   - Use Turso database URLs
   - Configure Turso credentials in frontend `.env`

**Update Frontend Configuration**:

Edit configuration files to match your database structure and preferences:
- Database connection strings
- Region/structure names
- Display preferences

**Run Streamlit App**:

```bash
streamlit run app.py
```

The app will be available at `http://localhost:8501`

### Step 11: Turso Remote Database Setup (Optional)

For production deployment with remote database access:

1. **Create Turso Account**:
   - Visit: https://turso.tech/
   - Sign up for free account

2. **Create Databases**:
   ```bash
   # Install Turso CLI
   curl -sSfL https://get.tur.so/install.sh | bash

   # Login
   turso auth login

   # Create databases
   turso db create market-data
   turso db create market-fittings
   turso db create eve-sde

   # Get connection strings
   turso db show market-data
   ```

3. **Generate Tokens**:
   ```bash
   turso db tokens create market-data
   turso db tokens create market-fittings
   turso db tokens create eve-sde
   ```

4. **Update .env**:
   - Add Turso URLs and tokens to `.env` file

5. **Initial Sync**:
   ```python
   from mkts_backend.config.config import DatabaseConfig

   db = DatabaseConfig("wcmkt")
   db.sync()  # Syncs local database to Turso
   ```

## Common Customizations

### Changing Market Structure

To switch to a different market structure:

1. Update `esi_config.py` with new structure/region/system IDs
2. Verify your ESI application has access (may need to re-authenticate)
3. Clear old market data or create new database
4. Run data collection: `uv run mkts-backend`

### Adding Custom Doctrines

1. Export fittings from Eve Online
2. Parse fittings using `parse_fits.py` utilities
3. Add to `wcfitting.db` database
4. Link doctrines in `doctrine_map` table
5. Run doctrine analysis: `uv run mkts-backend`

### Multi-Region Support

To track multiple regions:

1. Update `esi_config.py` with secondary market IDs
2. Add secondary watchlist if needed
3. Run separate data collection jobs
4. Frontend can display multi-region comparisons

## Troubleshooting Guide

### Authentication Issues

**Problem**: "CLIENT_ID environment variable is not set"
**Solution**: Verify `.env` file exists and contains CLIENT_ID

**Problem**: "Failed to refresh token"
**Solution**:
- Verify CLIENT_ID and SECRET_KEY are correct
- Check if REFRESH_TOKEN is valid (may need to regenerate)
- Ensure ESI application has correct scopes

**Problem**: "Forbidden" errors when fetching structure markets
**Solution**:
- Character must have docking access to structure
- Structure must allow market access
- ESI application needs `esi-markets.structure_markets.v1` scope

### Database Issues

**Problem**: "Database file does not exist"
**Solution**: Run `uv run mkts-backend` to create initial database

**Problem**: "Table not found"
**Solution**: Database schema may be outdated, check migrations or recreate

**Problem**: Turso sync fails
**Solution**:
- Verify Turso credentials in `.env`
- Check network connectivity
- Verify database exists on Turso

### Google Sheets Issues

**Problem**: "Failed to initialize Google Sheets client"
**Solution**:
- Verify JSON key file exists and path is correct
- Check GOOGLE_SHEET_KEY environment variable if using that method
- Verify service account has access to spreadsheet

**Problem**: "Insufficient permission" when updating sheets
**Solution**: Share spreadsheet with service account email with Editor permissions

### Data Collection Issues

**Problem**: No data being collected
**Solution**:
- Verify market structure has orders
- Check watchlist contains valid type_ids
- Review logs in `logs/mkts-backend.log`

**Problem**: Historical data not updating
**Solution**:
- Run with `--history` flag
- Verify region_id is correct
- Check ESI API status: https://esi.evetech.net/status.json

## Agent Workflow for User Support

When helping a user implement this system:

1. **Assess Requirements**:
   - What market structure/region are they tracking?
   - Do they need Google Sheets integration?
   - Local only or remote database?
   - Single structure or multi-region?

2. **Validate Prerequisites**:
   - Check Python version
   - Verify Eve Online account access
   - Confirm structure access permissions

3. **Guide Through Setup**:
   - Follow steps 1-11 in order
   - Don't skip configuration customization
   - Test each component before moving to next

4. **Test Data Collection**:
   - Run first data collection manually
   - Verify data appears in database
   - Check logs for errors

5. **Setup Automation**:
   - Configure scheduled runs
   - Test automated updates
   - Monitor for issues

6. **Configure Frontend**:
   - Setup database connection
   - Customize display settings
   - Test visualization

7. **Provide Documentation**:
   - Document custom configuration choices
   - Note any deviations from standard setup
   - Create troubleshooting notes for their specific setup

## Best Practices

1. **Start Local**: Begin with local-only setup before adding Turso/Sheets
2. **Small Watchlist**: Start with 10-20 items to test, expand gradually
3. **Test Data Flow**: Verify data flows from ESI > Database > Frontend
4. **Monitor Logs**: Check logs regularly for errors or warnings
5. **Backup Databases**: Regular backups of `.db` files
6. **Version Control**: Track configuration changes in git
7. **Security**: Never commit `.env` file or service account keys

## Additional Resources

- **ESI Documentation**: https://esi.evetech.net/ui/
- **Eve SDE**: https://developers.eveonline.com/resource/resources
- **Turso Documentation**: https://docs.turso.tech/
- **Google Sheets API**: https://developers.google.com/sheets/api
- **Streamlit Documentation**: https://docs.streamlit.io/

## Support and Contact

- Backend Repository Issues: https://github.com/OrthelT/mkts_backend/issues
- Frontend Repository Issues: https://github.com/OrthelT/wcmkts_new/issues
- Discord: orthel_toralen

## Architecture Summary for Agents

When explaining the system architecture:

```
Data Flow:
1. ESI API (Eve Online)
   ↓ (OAuth authenticated requests)
2. Backend Data Collection (mkts_backend)
   ↓ (SQLAlchemy ORM)
3. SQLite Database (wcmkt2.db)
   ↓ (Optional: libsql sync)
4. Turso Remote Database
   ↓ (SQLite connection)
5. Streamlit Frontend (wcmkts_new)
   ↓ (Visualization)
6. User Browser

Side Channel:
3. SQLite Database
   ↓ (gspread API)
7. Google Sheets
   ↓ (Manual viewing)
8. User
```

**Key Components**:
- **cli.py**: Main orchestration and entry point
- **esi_auth.py**: OAuth token management
- **esi_config.py**: Market configuration
- **models.py**: Database schema definitions
- **data_processing.py**: Statistics calculation
- **gsheets_config.py**: Google Sheets integration
- **config.py**: Database connection management

## Version Compatibility

- Python: 3.12+
- SQLAlchemy: 2.x
- libsql: Latest
- gspread: 5.x+
- pandas: 2.x
- Streamlit: 1.x+

## License and Disclaimer

This is an educational project for Eve Online market analysis. All Eve Online data is provided by CCP Games through their ESI API. Eve Online is a trademark of CCP Games.
