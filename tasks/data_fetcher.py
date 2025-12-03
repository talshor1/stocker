from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from writer import CsvWriter
from client import AlphaVantageClient
from service import TimeSeriesService

logger = logging.getLogger(__name__)

def fetch_and_save_intraday(cfg: Any, symbol: str, outfile: str) -> int:
    minutes = 5
    av = AlphaVantageClient(base_url=cfg.base_url, api_key=cfg.api_key)
    svc = TimeSeriesService(av)

    today = datetime.now(timezone.utc).date()
    logger.info(f"[{today}] Fetching {minutes}-minute intraday for {symbol}")

    candles = svc.last_n_intraday_minutes(symbol=symbol, minutes=minutes)
    if not candles:
        raise SystemExit("No data returned.")

    logger.info(f"Fetched {len(candles)} {minutes}-minute bars")
    CsvWriter.write_intraday(outfile, candles)
    logger.info(f"Saved {len(candles)} rows to {outfile}")

    av.close()
    return len(candles)
