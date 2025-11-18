# LLM Agent Guide: Eve Online Market Data System Setup

This guide helps LLM agents assist users in implementing the Eve Online Market Data Collection and Analysis System for their own market structures and regions.

## System Overview

This is a comprehensive market data system consisting of two repositories:

1. **mkts_backend** (this repo): Backend data collection, processing, and storage
   - Fetches market data from Eve Online ESI API
   - Processes and stores data in SQLite databases
   - Calculates market statistics and doctrine availability
   - Updates Google Sheets with market data
   - Supports local and remote (Turso) database sync

2. **wcmkts_new** (frontend): Streamlit web application for data visualization
   - Repository: https://github.com/OrthelT/wcmkts_new
   - Displays market statistics and trends
   - Shows doctrine/fitting availability
   - Provides interactive data exploration

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
