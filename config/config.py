import os
import sys
from sqlalchemy import create_engine, MetaData, inspect, text, select, update
from sqlalchemy.orm import Session
from sqlalchemy_orm import database
import pandas as pd
import time
import pathlib

import libsql
from dotenv import load_dotenv

import sqlite3 as sql
from esi.esi_auth import get_token
import requests
from config.logging_config import configure_logging

load_dotenv()

logger = configure_logging(__name__)

class DatabaseConfig:
    #configure this variable. It enables switching the db call across the application between wcmkt3 and wcmkt2.
    #this is useful for testing and development.
    wcdbmap = "wcmkt3"

    _db_paths = {
        "wcmkt3": "wcmkt3.db", #testing database
        "sde": "sde_info.db",
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

        if alias == "wcmkt":
            alias = self.wcdbmap
        elif alias == "wcmkt3" or alias == "wcmkt2":
            logger.warning(f"Database alias '{alias}' is deprecated. Configure wcdbmap in config.py to select wcmkt2 or wcmkt3 instead.")


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

        logger.info("connection established")
        conn = self.libsql_sync_connect
        with conn:
            logger.info("Syncing database...")
            result = conn.sync()
        conn.close()

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

    def get_table_length(self, table: str):
        with self.remote_engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
            return result[0]

    def get_status(self):
        status_dict = {}
        tables = self.get_table_list()
        for table in tables:
            with self.remote_engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
                status_dict[table] = result[0]
            conn.close()
        return status_dict

    def get_watchlist(self):
        engine = self.engine
        with engine.connect() as conn:
            df = pd.read_sql_table("watchlist", conn)
        conn.close()
        return df

    def verify_db_exists(self):
        path = pathlib.Path(self.path)
        if not path.exists():
            logger.error(f"Database file does not exist: {self.path}")
            self.sync()
        else:
            logger.info(f"Database file exists: {self.path}")

        return True



if __name__ == "__main__":

    pass
