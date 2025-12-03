from __future__ import annotations
import configparser
import sys
from logging import getLogger
from pathlib import Path
from models import AppConfig, MongoSettings
from kv import KeyVaultClient

logger = getLogger(__name__)

class ConfigLoader:
    SECTION = "alpha_vantage"

    @staticmethod
    def load(path: str = "config.ini") -> AppConfig:
        cfg = configparser.ConfigParser()
        p = Path(path)
        if p.exists():
            cfg.read(p)
            sec = cfg[ConfigLoader.SECTION] if ConfigLoader.SECTION in cfg else {}
            api_key  = (sec.get("api_key"))
            function = (sec.get("function"))
            base_url = (sec.get("base_url"))

            try:
                kv_client = KeyVaultClient()
                mongo_uri = kv_client.get_secret("mongo-url")
            except Exception as e:
                logger.error(f"Failed to get secret from Key Vault: {e}")
                sys.exit(1)

            if "mongo" in cfg:
                m = cfg["mongo"]
                mongo = MongoSettings(
                    uri=mongo_uri,
                    db=(m.get("db")),
                    candles_collection=(m.get("canldes_collection")),
                    tasks_collection=(m.get("tasks_collection")),
                )
            else:
                sys.exit("Missing mongo section in config.ini")

        else:
            sys.exit("Config file not found")

        if not api_key:
            raise SystemExit("Missing API key: set ALPHAVANTAGE_API_KEY or put it in config.ini")

        return AppConfig(
            api_key = api_key,
            function = function,
            base_url = base_url,
            mongo=mongo
        )
