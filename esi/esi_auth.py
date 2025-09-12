import os
import json
import time
from dotenv import load_dotenv
from requests_oauthlib import OAuth2Session
from config.logging_config import configure_logging

load_dotenv()
logger = configure_logging(__name__)

CLIENT_ID      = os.getenv("CLIENT_ID")
SECRET_KEY     = os.getenv("SECRET_KEY")
REFRESH_TOKEN  = os.getenv("REFRESH_TOKEN")
AUTH_URL       = "https://login.eveonline.com/v2/oauth/authorize"
TOKEN_URL      = "https://login.eveonline.com/v2/oauth/token"
CALLBACK_URI   = "http://localhost:8000/callback"
TOKEN_FILE     = "token.json"

def load_cached_token() -> dict | None:
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            return json.load(f)
    return None

def save_token(token: dict):
    with open(TOKEN_FILE, "w") as f:
        json.dump(token, f)

def get_oauth_session(token: dict | None, scope):
    extra = {"client_id": CLIENT_ID, "client_secret": SECRET_KEY}
    return OAuth2Session(
        CLIENT_ID,
        token=token,
        redirect_uri=CALLBACK_URI,
        scope=scope,
        auto_refresh_url=TOKEN_URL,
        auto_refresh_kwargs=extra,
        token_updater=save_token,
    )

def get_token(requested_scope):
    # Check if required environment variables are set
    if not CLIENT_ID:
        raise ValueError("CLIENT_ID environment variable is not set")
    if not SECRET_KEY:
        raise ValueError("SECRET_KEY environment variable is not set")
    if not REFRESH_TOKEN:
        raise ValueError("REFRESH_TOKEN environment variable is not set")

    # 1) Headless first-run: no cache → refresh from REFRESH_TOKEN
    token = load_cached_token()
    if not token:
        logger.info("No token.json → refreshing from GitHub secret")
        try:
            logger.info(f"Attempting to refresh token with CLIENT_ID: {CLIENT_ID[:8]}...")
            logger.info(f"Refresh token length: {len(REFRESH_TOKEN) if REFRESH_TOKEN else 'None'}")
            logger.info(f"Requested scope: {requested_scope}")

            token = OAuth2Session(CLIENT_ID, scope=requested_scope).refresh_token(
                TOKEN_URL,
                refresh_token=REFRESH_TOKEN,
                client_id=CLIENT_ID,
                client_secret=SECRET_KEY
            )
            save_token(token)
            logger.info("Token refreshed successfully")
            return token
        except Exception as e:
            logger.error(f"Failed to refresh token: {e}")
            logger.error(f"CLIENT_ID: {CLIENT_ID}")
            logger.error(f"REFRESH_TOKEN length: {len(REFRESH_TOKEN) if REFRESH_TOKEN else 'None'}")
            raise
    else:
        # 2) Cache exists → auto‑refresh if expired
        oauth = get_oauth_session(token, requested_scope)

        if token["expires_at"] < time.time():
            logger.info("Token expired → refreshing")
            try:
                oauth.refresh_token(TOKEN_URL, refresh_token=token["refresh_token"])
                new_token = oauth.token
                save_token(new_token)
                return new_token
            except Exception as e:
                logger.error(f"Failed to refresh cached token: {e}")
                raise
        else:
            return token

if __name__ == "__main__":
    pass