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

load_dotenv()

logger = configure_logging(__name__)


class DatabaseConfig:
    wcdbmap = "wcmkt2" #select wcmkt2 (production) or wcmkt3 (development)

    _db_paths = {
        "wcmkt3": "wcmkt3.db",
        "sde": "sdeinfo2.db",
        "fittings": "wcfitting.db",
        "wcmkt2": "wcmkt2.db",
    }

    _db_turso_urls = {
        "wcmkt3_turso": os.getenv("TURSO_WCMKT3_URL"),
        "sde_turso": os.getenv("TURSO_SDE2_URL"),
        "fittings_turso": os.getenv("TURSO_FITTING_URL"),
        "wcmkt2_turso": os.getenv("TURSO_WCMKT2_URL"),
    }

    _db_turso_auth_tokens = {
        "wcmkt3_turso": os.getenv("TURSO_WCMKT3_TOKEN"),
        "sde_turso": os.getenv("TURSO_SDE2_TOKEN"),
        "fittings_turso": os.getenv("TURSO_FITTING_TOKEN"),
        "wcmkt2_turso": os.getenv("TURSO_WCMKT2_TOKEN"),
    }

    def __init__(self, alias: str, dialect: str = "sqlite+libsql"):
        if alias == "wcmkt":
            alias = self.wcdbmap
        elif alias == "wcmkt3" or alias == "wcmkt2":
            logger.warning(
                f"Database alias '{alias}' is deprecated. Configure wcdbmap in config.py to select wcmkt2 or wcmkt3 instead."
            )

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
            logger.error(f"Database file does not exist: {self.path}")
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
