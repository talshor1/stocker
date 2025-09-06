from __future__ import annotations
from datetime import datetime, timezone
from typing import Any

from writer import CsvWriter
from client import AlphaVantageClient
from service import TimeSeriesService

def fetch_and_save_intraday(cfg: Any, symbol: str, days: int, minutes: int, outfile: str) -> int:
    av = AlphaVantageClient(base_url=cfg.base_url, api_key=cfg.api_key)
    svc = TimeSeriesService(av)

    today = datetime.now(timezone.utc).date()
    print(f"[{today}] Fetching {minutes}-minute intraday for {symbol}, last {days} days …")

    candles = svc.last_n_intraday_minutes(symbol=symbol, minutes=minutes, days=days)
    if not candles:
        raise SystemExit("No data returned.")

    print(f"Fetched {len(candles)} {minutes}-minute bars")
    CsvWriter.write_intraday(outfile, candles)
    print(f"Saved {len(candles)} rows to {outfile}")

    av.close()
    return len(candles)
