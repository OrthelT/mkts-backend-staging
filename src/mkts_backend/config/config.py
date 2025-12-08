import os
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
    _production_db_alias = settings["db"]["production_database_alias"]
    _production_db_file = settings["db"]["production_database_file"]
    _testing_db_alias = settings["db"]["testing_database_alias"]
    _testing_db_file = settings["db"]["testing_database_file"]
    _north_db_alias = settings["db"].get("north_database_alias", "wcmktnorth")
    _north_db_file = settings["db"].get("north_database_file", "wcmktnorth2.db")


    _db_paths = {
        _testing_db_alias: _testing_db_file,
        "sde": "sde.db",
        "fittings": "wcfitting.db",
        _production_db_alias: _production_db_file,
        _north_db_alias: _north_db_file,
    }

    _db_turso_urls = {
        _production_db_alias + "_turso": os.getenv("TURSO_WCMKTPROD_URL"),
        _testing_db_alias + "_turso": os.getenv("TURSO_WCMKTTEST_URL"),
        "sde_turso": os.getenv("TURSO_SDE_URL"),
        "fittings_turso": os.getenv("TURSO_FITTING_URL"),
        _north_db_alias + "_turso": os.getenv("TURSO_WCMKTNORTH_URL"),
    }

    _db_turso_auth_tokens = {
        _production_db_alias + "_turso": os.getenv("TURSO_WCMKTPROD_TOKEN"),
        _testing_db_alias + "_turso": os.getenv("TURSO_WCMKTTEST_TOKEN"),
        "sde_turso": os.getenv("TURSO_SDE_TOKEN"),
        "fittings_turso": os.getenv("TURSO_FITTING_TOKEN"),
        _north_db_alias + "_turso": os.getenv("TURSO_WCMKTNORTH_TOKEN"),
    }

    def __init__(self, alias: str, dialect: str = "sqlite+libsql"):
        if alias == "wcmkt":
            if self.settings["app"]["environment"] == "development":
                alias = self._testing_db_alias
            else:
                alias = self._production_db_alias

        if alias not in self._db_paths:
            raise ValueError(
                f"Unknown database alias '{alias}'. Available: {list(self._db_paths.keys())}"
            )

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
            self._remote_engine = create_engine(
                f"sqlite+{turso_url}?secure=true",
                connect_args={
                    "auth_token": auth_token,
                },
            )
        return self._remote_engine

    @property
    def libsql_local_connect(self):
        if self._libsql_connect is None:
            self._libsql_connect = libsql.connect(self.path)
        return self._libsql_connect

    @property
    def libsql_sync_connect(self):
        self._libsql_sync_connect = libsql.connect(
                f"{self.path}", sync_url=self.turso_url, auth_token=self.token
            )
        return self._libsql_sync_connect

    @property
    def sqlite_local_connect(self):
        if self._sqlite_local_connect is None:
            self._sqlite_local_connect = libsql.connect(self.path)
        return self._sqlite_local_connect

    def sync(self):
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
        end_info = json.loads(self.read_db_info())
        generation_change = end_info["generation"] - start_info["generation"]
        frames_synced = end_info["durable_frame_num"] - start_info["durable_frame_num"]
        logger.info(f"Generation change: {generation_change}")
        logger.info(f"Frames synced: {frames_synced}")
        logger.info("Sync complete")
        logger.info("=" * 80)

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

    def get_db_credentials_dicts(self):
        return {
            "turso_urls": self._db_turso_urls,
            "turso_tokens": self._db_turso_auth_tokens,
        }

if __name__ == "__main__":
    pass
