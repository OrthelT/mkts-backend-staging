import os, json, time
from dotenv import load_dotenv
from requests_oauthlib import OAuth2Session
from logging_config import configure_logging

load_dotenv()
logger = configure_logging(__name__)

CLIENT_ID      = os.getenv("CLIENT_ID")
SECRET_KEY     = os.getenv("SECRET_KEY")
REFRESH_TOKEN  = os.environ["REFRESH_TOKEN"]
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
    token["expires_at"] = time.time() + token["expires_in"]
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
    # 1) Headless first-run: no cache → refresh from REFRESH_TOKEN
    token = load_cached_token()
    if not token:
        logger.info("No token.json → refreshing from GitHub secret")
        token = OAuth2Session(CLIENT_ID).refresh_token(
            TOKEN_URL,
            refresh_token=REFRESH_TOKEN,
            client_id=CLIENT_ID,
            client_secret=SECRET_KEY,
            scope=requested_scope
        )
        save_token(token)
        return token

    # 2) Cache exists → auto‑refresh if expired
    oauth = get_oauth_session(token, requested_scope)
    if token["expires_at"] < time.time():
        logger.info("Token expired → refreshing")
        oauth.refresh_token(TOKEN_URL, refresh_token=token["refresh_token"])
    return oauth.token