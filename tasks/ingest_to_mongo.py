# tasks/ingest_csv_to_mongo.py
from __future__ import annotations
from typing import Any, Dict, List

from csv_to_mongo_helper import docs_from_csv
from db.mongo import get_repo

def ingest_csv_to_mongo(cfg: Any,
                        csv_path: str,
                        default_symbol: str,
                        batch_size: int = 1000) -> Dict[str, int]:

    repo = get_repo(uri=cfg.mongo.uri,
                    db=cfg.mongo.db,
                    collection=cfg.mongo.candles_collection)

    total_upserted = 0
    total_matched = 0
    total = 0

    batch: List[Dict] = []
    for doc in docs_from_csv(csv_path, default_symbol):
        batch.append(doc)
        if len(batch) >= batch_size:
            res = repo.upsert_many(batch, ordered=False)
            total_upserted += res["upserted"]
            total_matched  += res["matched"]
            total          += len(batch)
            batch.clear()

    if batch:
        res = repo.upsert_many(batch, ordered=False)
        total_upserted += res["upserted"]
        total_matched  += res["matched"]
        total          += len(batch)

    print(f"Mongo upserted: {total_upserted} | matched/updated: {total_matched} | processed: {total}")
    return {"upserted": total_upserted, "matched": total_matched, "processed": total}
