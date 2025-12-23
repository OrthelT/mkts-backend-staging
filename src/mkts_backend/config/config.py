import os
from numpy.char import upper
from sqlalchemy import create_engine, text
import pandas as pd
import pathlib

#os.environ.setdefault("RUST_LOG", "debug")

import libsql
from dotenv import load_dotenv
from mkts_backend.config.logging_config import configure_logging
from datetime import datetime, timezone
from time import perf_counter
import json
from pathlib import Path
import tomllib
import turso
from turso.sync import connect as turso_sync_connect

load_dotenv()
settings_file = "src/mkts_backend/config/settings.toml"

logger = configure_logging(__name__)

def load_settings(file_path: str = settings_file):
    with open(file_path, "rb") as f:
        settings = tomllib.load(f)
        logger.info(f"Settings loaded from {file_path}")
    return settings

class DatabaseConfig:
    settings = load_settings()
    _env = settings["app"]["environment"]
    _wcmkt_alias = settings[_env]["wcmkt"]
    _sde_alias = settings[_env]["sde"]
    _fittings_alias = settings[_env]["fittings"]
    _north_db_alias = settings["db"].get("north_database_alias", "wcmktnorth")
    _north_db_file = settings["db"].get("north_database_file", "wcmktnorth2.db")
    print(f"environment: {_env}")
    print(f"wcmkt alias: {_wcmkt_alias}")
    print(f"sde alias: {_sde_alias}")
    print(f"fittings alias: {_fittings_alias}")
    print(f"north db alias: {_north_db_alias}")
    print(f"north db file: {_north_db_file}")



    _db_turso_urls = {
        "wcmkt": os.getenv(f"TURSO_{_wcmkt_alias}_URL"),
        "sde": os.getenv(f"TURSO_{_sde_alias}_URL"),
        "fittings": os.getenv(f"TURSO_{_fittings_alias}_URL"),
        "wcmktnorth": os.getenv("TURSO_WCMKTNORTH_URL"),
    }

    _db_turso_auth_tokens = {
        "wcmkt": os.getenv(f"TURSO_{_wcmkt_alias}_TOKEN"),
        "sde": os.getenv(f"TURSO_{_sde_alias}_TOKEN"),
        "fittings": os.getenv(f"TURSO_{_fittings_alias}_TOKEN"),
        "wcmktnorth": os.getenv("TURSO_WCMKTNORTH_TOKEN"),
    }
    _db_aliases = {
        "wcmkt": _wcmkt_alias,
        "sde": _sde_alias,
        "fittings": _fittings_alias,
        "wcmktnorth": _north_db_alias,
    }
    
    def __init__(self, user_alias: str, dialect: str = "sqlite+libsql"):
        
        if user_alias not in self._db_aliases.keys():
            raise ValueError(
                f"Unknown database alias '{user_alias}'. Available: {list(self._db_aliases.keys())}"
            )

        self.alias = user_alias
        self.db_alias = self._db_aliases[self.alias]
        self.path = f"{self.db_alias}.db"
        self.turso_url = os.getenv(f"TURSO_{upper(self.db_alias)}_URL")
        self.token = os.getenv(f"TURSO_{upper(self.db_alias)}_TOKEN")
        self._engine = None
        self._remote_engine = None
        self._libsql_connect = None
        self._libsql_sync_connect = None
        self._turso_connect = None
        self._turso_remote_connect = None
        self.settings = load_settings()
        self._binding = self.settings["app"]["binding"]
        self._dialect = "sqlite+libsql" if self._binding == "libsql" else "sqlite"
        self.url = f"{self._dialect}:///{self.path}" 

    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_engine(self.url)
        return self._engine

    @property
    def remote_engine(self):
        if self._remote_engine is None:
            turso_url = self.turso_url
            auth_token = self.token
            self._remote_engine = create_engine(
                f"sqlite+{turso_url}?secure=true",
                connect_args={
                    "auth_token": auth_token,
                },
            )
        return self._remote_engine

    @property
    def libsql_local_connect(self):
        if self._libsql_connect is None and self._binding == "libsql":
            self._libsql_connect = libsql.connect(self.path)
            return self._libsql_connect
        elif self._binding == "turso":
            return self.turso_connect
        else:
            raise ValueError(f"Unknown binding: {self.settings['db']['binding']}")

    @property
    def libsql_sync_connect(self):
        if self._libsql_sync_connect is None and self._binding == "libsql":
            self._libsql_sync_connect = libsql.connect(
                    f"{self.path}", sync_url=self.turso_url, auth_token=self.token
                )
            return self._libsql_sync_connect
        elif self._binding == "turso":
            return self.turso_sync_connect
        else:
            raise ValueError(f"Unknown binding: {self.settings['db']['binding']}")

    @property
    def turso_connect(self):
        if self._turso_connect is None:
            self._turso_connect = turso.connect(self.path)
        return self._turso_connect

    @property
    def sqlite_local_connect(self):
        if self._sqlite_local_connect is None and self._binding == "libsql":
            self._sqlite_local_connect = libsql.connect(self.path)
        elif self._binding == "turso":
            return self.turso_connect
        else:
            raise ValueError(f"Unknown binding: {self.settings['db']['binding']}")
        return self._sqlite_local_connect


    def sync(self, push: bool = False):
        if self._binding == "turso":
            self.sync_turso(push=push)
        else:
            self.sync_libsql()

    def sync_libsql(self):
        conn = self.libsql_sync_connect
        start_info = json.loads(self.read_db_info())
        if start_info is not None:
            logger.info(f"Start info: {start_info}")
        else:
            logger.info("No start info found")
        sync_start_time = datetime.now()
        logger.info(f"Sync start time: {sync_start_time}")
        start_time = perf_counter()
        with conn:
            conn.sync()
        conn.close()
        end_time = perf_counter()
        logger.info(f"Sync time: {end_time - start_time:.1f} seconds")
        logger.info(f"Sync time: {(end_time - start_time)/60:.1f} minutes")
        logger.info(f"Sync end time: {datetime.now()}")
        logger.info("Sync complete")
        logger.info("=" * 80)

    def sync_turso(self, push: bool = False):
        conn = turso_sync_connect(self.path, remote_url=self.turso_url, auth_token=self.token)
        if push:
            logger.info("Pushing to remote database")
            conn.push()
            logger.info(f"Stats: {conn.stats()}")
        else:
            logger.info("Pulling from remote database")
            changed = conn.pull()
            logger.info(f"Changed: {changed}")
            logger.info(f"Stats: {conn.stats()}")

            conn.checkpoint()
            logger.info(f"Checkpoint complete")
            logger.info(f"Stats: {conn.stats()}")
        conn.close()
        logger.info("Sync complete")
        logger.info("=" * 80)
        return True

    def validate_sync(self) -> bool:
        logger.info(f"Validating sync for {self.alias}, url: {self.turso_url}, self.path: {self.path}")
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

    def get_table_list(self, local_only: bool = True) -> list[tuple]:
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
        if local_only:
            engine = self.engine
        else:
            engine = self.remote_engine

        with engine.connect() as conn:
            stmt = text(f"PRAGMA table_info({table_name})")
            result = conn.execute(stmt)
            columns = result.fetchall()
            if full_info:
                column_info = []
                for col in columns:
                    column_info.append(
                        {
                            "cid": col.cid,
                            "name": col.name,
                            "type": col.type,
                            "notnull": col.notnull,
                            "dflt_value": col.dflt_value,
                            "pk": col.pk,
                        }
                    )
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
            logger.warn(f"Database file does not exist: {self.path}")
            self.sync()
        else:
            logger.info(f"Database file exists: {self.path}")

        return True

    def read_db_info(self) -> str:
        info_path = f"{self.path}-info"
        info_path = Path(info_path)
        if not info_path.exists():
            return None
        with open(info_path, "r") as f:
            db_info = f.read()
        return db_info


if __name__ == "__main__":
    pass
