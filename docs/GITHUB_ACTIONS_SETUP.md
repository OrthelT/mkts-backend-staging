# GitHub Actions Setup Guide

This guide explains how to set up GitHub Actions to run the Eve Online market data collection CLI remotely.

## Overview

The GitHub Actions workflow is configured to:
- Run on a defined schedule
- Allow manual triggering with optional history processing
- Use remote databases (Turso) for data persistence
- Update Google Sheets with market data
- Store logs and output data as artifacts

## Setup Steps

### 1. Repository Secrets

Go to your GitHub repository → Settings → Secrets and variables → Actions, and add these secrets:

#### Required Secrets

- `CLIENT_ID`: Eve Online ESI application client ID
- `SECRET_KEY`: Eve Online ESI application secret key
- `REFRESH_TOKEN`: Eve Online SSO refresh token (for structure markets)
- `GOOGLE_SHEET_KEY`: Google Service Account JSON key (entire file content)

#### Optional Secrets (for remote database sync)

- `TURSO_MKT_URL` and `TURSO_MKT_TOKEN`
- `TURSO_SDE_URL` and `TURSO_SDE_TOKEN`
- `TURSO_FITTING_URL` and `TURSO_FITTING_TOKEN`
- `TURSO_WCMKT2_URL` and `TURSO_WCMKT2_TOKEN`

### 2. Eve Online ESI Application

1. Go to [Eve Online Developers](https://developers.eveonline.com/)
2. Create a new application
3. Set the callback URL to: `http://localhost:8080/callback`
4. Add the required scope: `esi-markets.structure_markets.v1`
5. Note down your Client ID and Secret Key

### 3. Google Service Account

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing one
3. Enable the Google Sheets API
4. Create a Service Account:
   - Go to IAM & Admin → Service Accounts
   - Create a new service account
   - Download the JSON key file
5. Share your Google Sheet with the service account email
6. Copy the entire JSON key file content to the `GOOGLE_SERVICE_ACCOUNT_KEY` secret

### 4. Turso Database (Optional)

If using remote database sync:

1. Sign up at [Turso](https://turso.tech/)
2. Create a new database
3. Get the database URL and auth token
4. Add them to repository secrets

## Workflow Configuration

### Schedule

The workflow runs on a schedule defined in `.github/workflows/market-data-collection.yml`.

### Manual Trigger

You can manually trigger the workflow:
1. Go to Actions tab in your repository
2. Select "Market Data Collection" workflow
3. Click "Run workflow"
4. Optionally enable "Include historical data processing"

### Timeout

The workflow has a timeout configured per job to prevent infinite runs.

## Output and Artifacts

### Logs

All logs are uploaded as artifacts and kept for 7 days:
- `market-data-logs-{run_number}`: Contains all application logs

### Generated Data

Output files are uploaded as artifacts:
- `market-data-output-{run_number}`: Contains JSON and CSV files

### Google Sheets

Market data updates can be published to your configured Google Sheet via the `GoogleSheetConfig` module.

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify Eve Online ESI credentials are correct
   - Check that the callback URL matches your ESI application

2. **Google Sheets Errors**
   - Ensure the service account has access to the spreadsheet
   - Verify the Google Service Account JSON is valid

3. **Database Connection Issues**
   - Check Turso credentials if using remote sync
   - Verify database URLs are correct

### Debugging

1. Check the workflow logs in the Actions tab
2. Download and examine the log artifacts
3. Test the script locally with the same environment variables

## Security Considerations

- Secrets are automatically masked in logs
- Sensitive files are cleaned up after each run
- Service account keys are not stored in the repository
- Database credentials are handled securely

## Customization

### Changing Schedule

Edit the cron expression in `.github/workflows/market-data-collection.yml`.

### Adding More Triggers

Add additional trigger conditions in the workflow `on:` section.

### Modifying Artifact Retention

Change `retention-days` in the upload-artifact step.

## Monitoring

### Workflow Status

- Check the Actions tab for workflow run status
- Set up GitHub notifications for failed runs
- Monitor logs for errors or warnings

### Data Quality

- Review generated artifacts for completeness
- Check Google Sheets for data updates
- Monitor database sync status in logs

## Running the CLI

The refactored project exposes a console script:

```bash
uv run mkts-backend            # run market collection
uv run mkts-backend --history  # include historical data processing
```

## Cost Considerations

- GitHub Actions minutes are consumed for each run
- Turso has usage limits on free tier
- Google Sheets API has rate limits
- Consider optimizing schedule based on your needs
