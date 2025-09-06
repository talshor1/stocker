# mongo_client.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Mapping, Any, List, Dict, Optional, Tuple
from datetime import datetime, timezone
from urllib.parse import urlparse
import os

from pymongo import MongoClient, ASCENDING, UpdateOne
import certifi


@dataclass(frozen=True)
class MongoConfig:
    uri: str
    db: str
    collection: str
    app_name: Optional[str] = "stocker-app"


def _mask(uri: str) -> str:
    u = urlparse(uri)
    if "@" in u.netloc:
        _, host = u.netloc.split("@", 1)
        return f"{u.scheme}://***:***@{host}{u.path or ''}{'?' + u.query if u.query else ''}"
    return uri


class MongoPriceRepo:
    def __init__(self, cfg: MongoConfig):
        print("Connecting to Mongo:", _mask(cfg.uri))

        self.client = MongoClient(
            cfg.uri,
            appname=cfg.app_name or "stocker-app",
            serverSelectionTimeoutMS=60000,
            connectTimeoutMS=30000,
            socketTimeoutMS=30000,
            tlsCAFile=certifi.where(),
        )
        self.db = self.client[cfg.db]
        self.col = self.db[cfg.collection]

    def ping(self) -> bool:
        try:
            self.client.admin.command("ping")
            return True
        except Exception as exc:
            print(f"Ping error: {exc}")
            return False

    def close(self) -> None:
        self.client.close()

    def upsert_many(self, docs: Iterable[Mapping[str, Any]], ordered: bool = False) -> Dict[str, int]:
        ops: List[UpdateOne] = []
        for d in docs:
            sym = d.get("symbol")
            ts = d.get("ts")
            if not sym or not isinstance(sym, str):
                raise ValueError("doc missing 'symbol' (str)")
            if not isinstance(ts, datetime):
                raise ValueError("doc missing 'ts' (datetime)")
            if ts.tzinfo is None:
                d = {**d, "ts": ts.replace(tzinfo=timezone.utc)}
            ops.append(UpdateOne({"symbol": d["symbol"], "ts": d["ts"]},
                                 {"$set": dict(d)}, upsert=True))
        if not ops:
            return {"matched": 0, "upserted": 0}
        res = self.col.bulk_write(ops, ordered=ordered)
        return {"matched": res.matched_count, "upserted": len(res.upserted_ids or {})}

    def fetch_range(self, symbol: str, start: datetime, end: datetime) -> List[Dict[str, Any]]:
        if start.tzinfo is None: start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None: end = end.replace(tzinfo=timezone.utc)
        cur = self.col.find(
            {"symbol": symbol, "ts": {"$gte": start, "$lte": end}},
            projection={"_id": 0}
        ).sort("ts", ASCENDING)
        return list(cur)

    def latest_n(self, symbol: str, n: int) -> List[Dict[str, Any]]:
        rows = list(self.col.find({"symbol": symbol}, projection={"_id": 0})
                    .sort("ts", -1).limit(n))
        rows.reverse()
        return rows

_repo: Optional[MongoPriceRepo] = None
_repo_key: Optional[Tuple[str, str, str, Optional[str]]] = None  # (uri, db, collection, app_name)

def _resolve_cfg(uri: Optional[str], db: Optional[str], coll: Optional[str], app_name: Optional[str]) -> MongoConfig:
    uri = uri or os.getenv("MONGO_URI")
    db = (db or os.getenv("MONGO_DB") or "stocker").strip()
    coll = (coll or os.getenv("MONGO_COLLECTION") or "prices").strip()
    if not uri:
        raise SystemExit("Missing MONGO_URI (env or pass to get_repo)")
    return MongoConfig(uri=uri, db=db, collection=coll, app_name=app_name or "stocker-app")

def get_repo(uri: Optional[str] = None,
             db: Optional[str] = None,
             collection: Optional[str] = None,
             app_name: Optional[str] = "stocker-app") -> MongoPriceRepo:
    global _repo, _repo_key
    cfg = _resolve_cfg(uri, db, collection, app_name)
    key = (cfg.uri, cfg.db, cfg.collection, cfg.app_name)

    if _repo is None:
        repo = MongoPriceRepo(cfg)
        if not repo.ping():
            raise SystemExit("Mongo ping failed – check URI/network/firewall/TLS trust.")
        _repo, _repo_key = repo, key
        return _repo

    if _repo_key != key:
        raise RuntimeError("Mongo repo already initialized with a different config. Call reset_repo() first.")
    return _repo

def reset_repo() -> None:
    global _repo, _repo_key
    if _repo is not None:
        _repo.close()
    _repo = None
    _repo_key = None
