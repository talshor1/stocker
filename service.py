# service.py
from __future__ import annotations

import logging
from typing import List, Dict, Any
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

EXCHANGE_TZ = ZoneInfo("America/New_York")
logger = logging.getLogger(__name__)

class TimeSeriesService:
    def __init__(self, client):
        self.client = client

    def last_n_intraday_minutes(self, *, symbol: str, minutes: int) -> List[Dict[str, Any]]:
        logger.info(f"Fetching {minutes}-minute intraday bars for {symbol}")
        interval = f"{minutes}min"

        logger.info("Sending request to AV...")
        recent = self.client.fetch_time_series_intraday(symbol=symbol, interval=interval)
        logger.info("Received response from AV")

        bars = self._parse_intraday_payload(symbol, recent)
        logger.info(f"Fetched {len(bars)} bars")
        return bars

    @staticmethod
    def _parse_intraday_payload(symbol: str, payload: Dict[str, Any] | str) -> List[Dict[str, Any]]:
        """Convert AV intraday or daily JSON into normalized dicts with UTC timestamps."""
        import json

        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                logger.error("Invalid JSON from AV")
                return []

        if not isinstance(payload, dict):
            return []

        # Handle AV error messages
        if "Error Message" in payload:
            logger.error(f"Alpha Vantage error: {payload['Error Message']}")
            return []

        # Find the time series block
        block = next((v for k, v in payload.items() if "Time Series" in k), None)
        if block is None:
            logger.error(f"No Time Series block in payload")
            return []

        rows = []

        for ts_str, fields in block.items():
            try:
                # Detect timestamp format
                if " " in ts_str:  # intraday format
                    dt_local = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=EXCHANGE_TZ)
                else:  # daily format
                    dt_local = datetime.strptime(ts_str, "%Y-%m-%d").replace(tzinfo=EXCHANGE_TZ)

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

            except Exception as e:
                logger.error(f"Failed to parse bar {ts_str} for {symbol}: {e} fields={fields}")
                continue

        return rows
