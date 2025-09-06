# scheduler.py
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Any
from ctx.ctx_reader import CtxReader

from db.mongo import MongoConfig
from tasks.data_fetcher import fetch_and_save_intraday
from tasks.ingest_to_mongo import ingest_csv_to_mongo


def _outfile_from_ctx(ctx: dict, base_dir: str = "DATA") -> str:
    sym = ctx["symbol"]
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    p = Path(base_dir) / sym / day
    p.mkdir(parents=True, exist_ok=True)
    return str(p / f"{sym}_m{ctx['minute_interval']}_d{ctx['days_back']}.csv")


DISPATCH: Dict[str, Callable[[Any, dict], None]] = {
    "fetch_and_save_intraday": lambda cfg, ctx: fetch_and_save_intraday(
        cfg,
        symbol=ctx["symbol"],
        days=int(ctx["days_back"]),
        minutes=int(ctx["minute_interval"]),
        outfile=_outfile_from_ctx(ctx),
    ),
    "ingest_csv_to_mongo": lambda cfg, ctx: ingest_csv_to_mongo(
        cfg,
        csv_path=_outfile_from_ctx(ctx),
        default_symbol=ctx["symbol"],
    ),
}

def schedule(cfg) -> None:
    mongo_cfg = MongoConfig(
        uri = cfg.mongo.uri,
        db = cfg.mongo.db,
        collection = cfg.mongo.ctx_collection,
    )
    ctxReader = CtxReader(mongo_cfg)

    while True:
        jobs = ctxReader.list_jobs()

        for job in jobs:
            op = job.get("op")
            fn = DISPATCH.get(op)
            if not fn:
                print(f"{op} is not supported")
                continue

            fn(cfg, job)
        sys.exit(0)
