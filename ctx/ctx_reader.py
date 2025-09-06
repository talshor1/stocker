from __future__ import annotations
from dataclasses import dataclass
from typing import List, Any, Dict

import certifi
from pymongo import MongoClient


@dataclass(frozen=True)
class MongoConfig:
    uri: str
    db: str
    collection: str


class CtxReader:
    def __init__(self, cfg: MongoConfig):
        self._client = MongoClient(cfg.uri, tlsCAFile=certifi.where())
        self._coll = self._client[cfg.db][cfg.collection]

    def list_jobs(self) -> List[Dict[str, Any]]:
        q = {"is_enabled": True}
        cur = self._coll.find(q)
        return list(cur)

    @staticmethod
    def describe(ctx: Dict[str, Any]) -> str:
        return (
            f"ctx({ctx.get('_id')}, "
            f"op={ctx.get('op')}, "
            f"symbol={ctx.get('symbol')}, "
            f"next_run={ctx.get('next_run')}, "
            f"enabled={ctx.get('is_enabled')})"
        )
