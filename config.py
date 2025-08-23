import os
import sys
from sqlalchemy import create_engine, MetaData, inspect, text, select, update
from sqlalchemy.orm import Session
from sqlalchemy_orm import database
import pandas as pd

import libsql
from dotenv import load_dotenv
from mkt_models import *
from sde_models import *
import sqlite3 as sql
from ESI_OAUTH_FLOW import get_token
import requests
from logging_config import configure_logging


load_dotenv()

logger = configure_logging(__name__)

class DatabaseConfig:
    _db_paths = {
        "wcmkt3": "wcmkt3.db", #testing database
        "sde": "sdeinfo.db",
        "fittings": "wcfitting.db",
        "wcmkt2": "wcmkt2.db",
    }

    _db_turso_urls = {
        "wcmkt3_turso": os.getenv("TURSO_MKT_URL"),
        "sde_turso": os.getenv("TURSO_SDE_URL"),
        "fittings_turso": os.getenv("TURSO_FITTING_URL"),
        "wcmkt2_turso": os.getenv("TURSO_WCMKT2_URL"),
    }

    _db_turso_auth_tokens = {
        "wcmkt3_turso": os.getenv("TURSO_MKT_TOKEN"),
        "sde_turso": os.getenv("TURSO_SDE_TOKEN"),
        "fittings_turso": os.getenv("TURSO_FITTING_TOKEN"),
        "wcmkt2_turso": os.getenv("TURSO_WCMKT2_TOKEN"),
    }

    def __init__(self, alias: str, dialect: str = "sqlite+libsql"):
        if alias not in self._db_paths:
            raise ValueError(f"Unknown database alias '{alias}'. "
                             f"Available: {list(self._db_paths.keys())}")

        self.alias = alias
        self.path = self._db_paths[alias]
        self.url = f"{dialect}:///{self.path}"
        self.turso_url = self._db_turso_urls[f"{self.alias}_turso"]
        self.token = self._db_turso_auth_tokens[f"{self.alias}_turso"]
        self._engine = None
        self._remote_engine = None
        self._libsql_connect = None
        self._libsql_sync_connect = None
        self._sqlite_local_connect = None

    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_engine(self.url)
        return self._engine
    
    @property
    def remote_engine(self):
        if self._remote_engine is None:
            turso_url = self._db_turso_urls[f"{self.alias}_turso"]
            auth_token = self._db_turso_auth_tokens[f"{self.alias}_turso"]
            self._remote_engine = create_engine(f"sqlite+{turso_url}?secure=true", connect_args={"auth_token": auth_token,},)
        return self._remote_engine

    @property
    def libsql_local_connect(self):
        if self._libsql_connect is None:
            self._libsql_connect = libsql.connect(self.path)
        return self._libsql_connect
    
    @property
    def libsql_sync_connect(self):
        if self._libsql_sync_connect is None:
            self._libsql_sync_connect = libsql.connect(f"{self.path}", sync_url = self.turso_url, auth_token=self.token)
        return self._libsql_sync_connect
    
    @property
    def sqlite_local_connect(self):
        if self._sqlite_local_connect is None:
            self._sqlite_local_connect = sql.connect(self.path)
        return self._sqlite_local_connect
    
    def sync(self):
        connection = self.libsql_sync_connect
        logger.info("connection established")
        with connection as conn:
            logger.info("Syncing database...")
            result = conn.sync()
            logger.info(f"sync result: {result}")
        conn.close()
        if self.validate_sync():
            logger.info("Sync complete")
            sync_state = "successful"
        else:
            logger.error("Validation test failed.")
            sync_state = "failed"
        return sync_state
            
    
    def validate_sync(self)-> bool:
        alias = self.alias
        with self.remote_engine.connect() as conn:
            result = conn.execute(text("SELECT MAX(last_update) FROM marketstats")).fetchone()
            remote_last_update = result[0]
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT MAX(last_update) FROM marketstats")).fetchone()
            local_last_update = result[0]
        logger.info(f"remote_last_update: {remote_last_update}")
        logger.info(f"local_last_update: {local_last_update}")
        validation_test = remote_last_update == local_last_update
        logger.info(f"validation_test: {validation_test}")
        return validation_test
        

    def get_table_list(self, local_only: bool = True)-> list[tuple]:
        if local_only:
            engine = self.engine
            with engine.connect() as conn:
                stmt = text("PRAGMA table_list")
                result = conn.execute(stmt)
                tables = result.fetchall()
                table_list = [table.name for table in tables if "sqlite" not in table.name]
                return table_list
        else:
            engine = self.remote_engine
            with engine.connect() as conn:
                stmt = text("PRAGMA table_list")
                result = conn.execute(stmt)
                tables = result.fetchall()
                table_list = [table.name for table in tables if "sqlite" not in table.name]
                return table_list

    def get_table_columns(self, table_name: str, local_only: bool = True, full_info: bool = False) -> list[dict]:
        """
        Get column information for a specific table.
        
        Args:
            table_name: Name of the table to inspect
            local_only: If True, use local database; if False, use remote database
            
        Returns:
            List of dictionaries containing column information
        """
        if local_only:
            engine = self.engine
        else:
            engine = self.remote_engine
            
        with engine.connect() as conn:
            # Use string formatting for PRAGMA since it doesn't support parameterized queries well
            stmt = text(f"PRAGMA table_info({table_name})")
            result = conn.execute(stmt)
            columns = result.fetchall()
            if full_info:
                column_info = []
                for col in columns:
                    column_info.append({
                    "cid": col.cid,
                    "name": col.name,
                    "type": col.type,
                    "notnull": col.notnull,
                    "dflt_value": col.dflt_value,
                    "pk": col.pk
                })
            else:
                column_info = [col.name for col in columns]
            
            
            return column_info

class GoogleSheetConfig:
    def __init__(self):
        self.google_private_key_file = "wcdoctrines-1f629d861c2f.json" #name of your google service account key file
        self.google_sheet_url = "https://docs.google.com/spreadsheets/d/1frGs3XzB7kooVoN-rqRUfoYX3k3FIFgo1ZDAypzc-pI/edit?gid=1738061156#gid=1738061156" #url of your google sheet
        self.sheet_name = "nakah_market_data" #name of the sheet you want to update

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
    """

    _region_ids = {"primary_region_id": 10000003, "secondary_region_id": 10000001}
    _system_ids = {"primary_system_id": 30000240, "secondary_system_id": 30000072}
    _structure_ids = {"primary_structure_id": 1035466617946, "secondary_structure_id": 60014068}
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
        self.compatibility_date = "2025-08-20"

    def token(self, scope: str = "esi-markets.structure_markets.v1"):
        return get_token(scope)

    @property
    def market_orders_url(self):
        if self.alias == "primary":
            return f"https://esi.evetech.net/latest/markets/structures/{self.structure_id}"
        elif self.alias == "secondary":
            return f"https://esi.evetech.net/latest/markets/{self.region_id}/orders"

    def headers(self, etag: str = None)-> dict:

        if self.alias == "primary":
            token = self.token(scope = 'esi-markets.structure_markets.v1')
            auth_token = f"Bearer {token['access_token']}"
            return {
        "Accept-Language": "en",
        "If-None-Match": etag,
        "X-Compatibility-Date": self.compatibility_date,
        "X-Tenant": "",
        "Accept": "application/json",
        "User-Agent": self.user_agent,
        "Authorization": auth_token,
    }
        elif self.alias == "secondary":
            return {
        "Accept-Language": "en",
        "If-None-Match": etag,
        "X-Compatibility-Date": self.compatibility_date,
        "X-Tenant": "",
        "Accept": "application/json",
        "User-Agent": self.user_agent
    }
        else:
            raise ValueError(f"Invalid alias: {self.alias}. Valid aliases are: {self._valid_aliases}")


    def market_orders(self, page: int = 1, order_type: str = "all", etag: str = None)-> requests.Response:
        """
        order_type: str = "all" | "buy" | "sell" (default is "all", only used for secondary market)
        page: int = 1 is the default page number. This can be used to fetch a single page of orders, or as an argument dynamically updated in a loop. 
        etag: str = None is the etag of the last response for the requested page. This is used to optionally check for changes in the market orders. The esi will return a 304 if the etag is the same as the last response.

        Returns:
            requests.Response: Response object containing the market orders
        Raises:
            ValueError: If the alias is invalid
        """
        
        if self.alias == "primary":
            querystring = {"page": page}     
        elif self.alias == "secondary":
            querystring = {"page": page, "order_type": order_type}
        else:
            raise ValueError(f"Invalid alias: {self.alias}. Valid aliases are: {self._valid_aliases}")
        
        headers = self.headers(etag = etag)
        url = self.market_orders_url
        response = requests.get(url, headers=headers, params=querystring)
        return response



def verbose_sync(db: DatabaseConfig):
    sync_state = db.sync()
    print("---------------------------")
    print(f"sync_state: {sync_state}")
    print("---------------------------")

if __name__ == "__main__":
    pass
