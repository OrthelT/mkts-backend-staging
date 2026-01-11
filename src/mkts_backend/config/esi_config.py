from mkts_backend.esi.esi_auth import get_token
from mkts_backend.config.logging_config import configure_logging
from mkts_backend.config.config import load_settings, settings_file

logger = configure_logging(__name__)


settings = load_settings(settings_file)

class ESIConfig:
    """ESI configuration for primary and secondary markets."""

    _region_ids = {"primary_region_id": settings["market_data"]["primary_region_id"], "deployment_region_id": settings["market_data"]["deployment_region_id"]}
    _system_ids = {"primary_system_id": settings["market_data"]["primary_system_id"], "deployment_system_id": settings["market_data"]["deployment_system_id"]}
    _structure_ids = {"primary_structure_id": settings["market_data"]["primary_structure_id"], "deployment_structure_id": settings["market_data"]["deployment_structure_id"]}
    _valid_aliases = ["primary", "deployment"] #primary is the default market, deployment is the deployment market, and is currently unused.
    _shortcut_aliases = {"4h": "primary", "deployment": "deployment"}
    _names = {"primary": settings["market_data"]["primary_market_name"], 
              "deployment": settings["market_data"]["deployment_market_name"]} #deployment is currently unused, but will be used in the future.

    def __init__(self, alias: str = "primary"):
        alias = alias.lower()
        if alias not in self._valid_aliases and alias not in self._shortcut_aliases:
            raise ValueError(
                f"Invalid alias: {alias}. Valid aliases are: {self._valid_aliases} or {list(self._shortcut_aliases.keys())}"
            )
        elif alias in self._shortcut_aliases:
            self.alias = self._shortcut_aliases[alias]
        else:
            self.alias = alias
        self.name = self._names[self.alias]
        self.region_id = self._region_ids[f"{self.alias}_region_id"]
        self.system_id = self._system_ids[f"{self.alias}_system_id"]
        self.structure_id = self._structure_ids[f"{self.alias}_structure_id"]

        self.user_agent = settings["esi"]["user_agent"]
        self.compatibility_date = settings["esi"]["compatibility_date"]

    def token(self, scope: str = "esi-markets.structure_markets.v1"):
        return get_token(scope)

    @property
    def market_orders_url(self):
        if self.alias == "primary":
            return f"https://esi.evetech.net/markets/structures/{self.structure_id}"
        elif self.alias == "deployment":
            return f"https://esi.evetech.net/markets/{self.region_id}/orders"

    @property
    def market_history_url(self):
        return f"https://esi.evetech.net/markets/{self.region_id}/history"

    @property
    def headers(self, etag: str = None) -> dict:
        if self.alias == "primary":
            token = self.token()
            return {
                "Accept-Language": "en",
                "If-None-Match": f"{etag}",
                "X-Compatibility-Date": self.compatibility_date,
                "X-Tenant": "tranquility",
                "Accept": "application/json",
                "Authorization": f"Bearer {token['access_token']}",
            }
        elif self.alias == "deployment":
            return {
                "Accept-Language": "en",
                "If-None-Match": etag,
                "X-Compatibility-Date": self.compatibility_date,
                "Accept": "application/json",
                "User-Agent": self.user_agent,
            }
        else:
            raise ValueError(f"Invalid alias: {self.alias}. Valid aliases are: {self._valid_aliases}")
