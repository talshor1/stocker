import os, time, configparser
import ssl
import sys

import certifi
import redis

from logging import getLogger
from kv import KeyVaultClient

logger = getLogger(__name__)

r = None
LIMIT = None
WINDOW = None

def getRedis():
    global r
    global LIMIT
    global WINDOW
    if r is not None:
        return r

    logger.info("Initializing redis rate limit")

    kv_client = KeyVaultClient()
    redis_key = kv_client.get_secret("redis-key")

    cfg = configparser.ConfigParser()
    cfg.read(os.getenv("APP_CONFIG", "config.ini"))

    if "redis" in cfg:
        redis_config = cfg["redis"]
        r = redis.Redis(
            host = redis_config.get("host"),
            port = redis_config.getint("port"),
            password = redis_key,
            ssl = True,
            decode_responses = True,
            ssl_cert_reqs=ssl.CERT_REQUIRED,
            ssl_ca_certs=certifi.where(),
            ssl_check_hostname=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            health_check_interval=30,
        )
        LIMIT = cfg.getint("redis", "limit", fallback=60)
        WINDOW = cfg.getint("redis", "window", fallback=60)
    else:
        sys.exit("Missing redis section in config.ini")
    return r

def allow(symbol: str) -> bool:
    r = getRedis()
    now = int(time.time())
    bucket = now // WINDOW
    key = f"stocker:{symbol}:{bucket}"
    logger.info(f"Checking rate limit for {symbol} in bucket {bucket}")
    current = r.incr(key)
    if current == 1:
        r.expire(key, WINDOW)

    return current <= LIMIT
