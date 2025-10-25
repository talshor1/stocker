from typing import Dict, Callable, Any

from scheduler.scheduler import _outfile_from_ctx
from tasks.data_fetcher import fetch_and_save_intraday
from tasks.ingest_to_mongo import ingest_csv_to_mongo

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