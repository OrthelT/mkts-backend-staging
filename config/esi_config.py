from esi.esi_auth import get_token
from .logging_config import configure_logging
logger = configure_logging(__name__)


class ESIConfig:
    """Current configuration for the primary and secondary markets:
    Primary market: 4-HWWF Keepstar (player-owned citadel)
    Secondary market: Nakah I - Moon 1 - Thukker Mix Factory (NPC structure)

    Note:
    The primary market must be a player-owned citadel market. The secondary market must be an NPC structure.
    Citadels and NPC structure markets use different endpoints in the ESI, and have different headers and authentication requirements.
    A valid esi token is required for the primary market. The secondary market does not require a token.

    A typical configuration might be to set the primary market as your nullsec staging citadel and the secondary market as Jita 4-4.

    Configure the variables below as needed. You can optionally define a shortcut alias for the primary or secondary market. if it helps you remember the alias. Set names to match your aliases, this is primarily used for logging.

    Note: Secondary market is no longer used in the current deployment, and will need some refactoring to be used again.
    """

    _region_ids = {"primary_region_id": 10000003, "secondary_region_id": None}
    _system_ids = {"primary_system_id": 30000240, "secondary_system_id": None}
    _structure_ids = {"primary_structure_id": 1035466617946, "secondary_structure_id": None}
    _valid_aliases = ["primary", "secondary"]
    _shortcut_aliases = {"4h": "primary", "nakah": "secondary"}
    _names = {"primary": "4-HWWF Keepstar", "secondary": "Nakah I - Moon 1 - Thukker Mix Factory"}

    def __init__(self, alias: str):
        # Here we handle the alias input, by converting to lowercase and checking to ensure it is a valid alias or a shortcut.
        alias = alias.lower()
        if alias not in self._valid_aliases and alias not in self._shortcut_aliases:
            raise ValueError(f"Invalid alias: {alias}. Valid aliases are: {self._valid_aliases} or {list(self._shortcut_aliases.keys())}")
        elif alias in self._shortcut_aliases:
            self.alias = self._shortcut_aliases[alias]
        else:
            self.alias = alias
        self.name = self._names[self.alias]
        self.region_id = self._region_ids[f"{self.alias}_region_id"]
        self.system_id = self._system_ids[f"{self.alias}_system_id"]
        self.structure_id = self._structure_ids[f"{self.alias}_structure_id"]

        self.user_agent = 'wcmkts_backend/2.1dev, orthel.toralen@gmail.com, (https://github.com/OrthelT/wcmkts_backend)'
        self.compatibility_date = "2025-08-26"

    def token(self, scope: str = "esi-markets.structure_markets.v1"):
        return get_token(scope)

    @property
    def market_orders_url(self):
        if self.alias == "primary":
            return f"https://esi.evetech.net/markets/structures/{self.structure_id}"
        elif self.alias == "secondary":
            return f"https://esi.evetech.net/markets/{self.region_id}/orders"

    @property
    def market_history_url(self):
        return f"https://esi.evetech.net/markets/{self.region_id}/history"

    @property
    def headers(self, etag: str = None)-> dict:

        if self.alias == "primary":
            token = self.token()

            return {
        "Accept-Language": "en",
        "If-None-Match": f"{etag}",
        "X-Compatibility-Date": self.compatibility_date,
        "X-Tenant": "tranquility",
        "Accept": "application/json",
        "Authorization": f"Bearer {token['access_token']}"
    }
        elif self.alias == "secondary":
            return {
        "Accept-Language": "en",
        "If-None-Match": etag,
        "X-Compatibility-Date": self.compatibility_date,
        "Accept": "application/json",
        "User-Agent": self.user_agent
    }
        else:
            raise ValueError(f"Invalid alias: {self.alias}. Valid aliases are: {self._valid_aliases}")
