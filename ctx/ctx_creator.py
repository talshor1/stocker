#!/usr/bin/env python3
from datetime import datetime, timezone
from typing import Dict, List, Any

from config.config import ConfigLoader
from db.mongo import get_repo
from models.ctx import Ctx


def utc_now():
    return datetime.now(timezone.utc)

def build_ctx(symbol: str, task_interval_minutes: int) -> dict:
    ctx = Ctx(symbol=symbol, task_interval=task_interval_minutes)
    return ctx.to_dict()

def main():
    cfg = ConfigLoader.load("../config.ini")
    repo = get_repo(cfg.mongo.uri, cfg.mongo.db, cfg.mongo.ctx_collection)
    ctx_col = repo.col

    docs: List[Dict[str, Any]] = [build_ctx("MSFT", 1), build_ctx("AAPL", 1)]

    result = ctx_col.insert_many(docs)
    print(f"Inserted {len(result.inserted_ids)} ctx docs:", result.inserted_ids)

if __name__ == "__main__":
    main()
