# service.py
from __future__ import annotations
from typing import List, Dict, Any
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

EXCHANGE_TZ = ZoneInfo("America/New_York")

class TimeSeriesService:
    def __init__(self, client):
        self.client = client

    def last_n_intraday_minutes(self, *, symbol: str, minutes: int, days: int) -> List[Dict[str, Any]]:
        interval = f"{minutes}min"

        recent = self.client.fetch_time_series_intraday(
            symbol=symbol, interval=interval, outputsize="full", month=None
        )
        bars = self._parse_intraday_payload(symbol, recent)

        if days > 30:
            first_of_month = datetime.now(timezone.utc).date().replace(day=1)
            prev_month = (first_of_month - timedelta(days=1)).strftime("%Y-%m")
            older = self.client.fetch_time_series_intraday(
                symbol=symbol,
                interval=interval,
                outputsize="full",
                month=prev_month
            )
            bars.extend(self._parse_intraday_payload(symbol, older))

        dedup = {(b["ts"], b["symbol"]): b for b in bars}
        bars = sorted(dedup.values(), key=lambda b: b["ts"])
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        return [b for b in bars if b["ts"] >= cutoff]

    @staticmethod
    def _parse_intraday_payload(symbol: str, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Convert AV intraday JSON into normalized dicts with UTC timestamps."""
        block = next((v for k, v in payload.items() if "Time Series" in k), {})
        rows: List[Dict[str, Any]] = []
        for ts_local, fields in block.items():
            # e.g. "2025-08-22 15:35:00" in US/Eastern -> UTC
            dt_local = datetime.strptime(ts_local, "%Y-%m-%d %H:%M:%S").replace(tzinfo=EXCHANGE_TZ)
            ts_utc = dt_local.astimezone(timezone.utc)
            rows.append({
                "symbol": symbol,
                "ts": ts_utc,
                "open": float(fields["1. open"]),
                "high": float(fields["2. high"]),
                "low": float(fields["3. low"]),
                "close": float(fields["4. close"]),
                "volume": int(fields["5. volume"]),
            })
        return rows
