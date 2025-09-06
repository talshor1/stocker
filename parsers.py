from __future__ import annotations
from typing import Dict, List
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from models.candle import Candle, IntradayCandle

EXCHANGE_TZ = ZoneInfo("America/New_York")

class DailyParser:
    @staticmethod
    def parse_timeseries(data: Dict, symbol: str) -> List[Candle]:
        ts = data.get("Time Series (Daily)") or {}
        candles: List[Candle] = []
        for ds, entry in ts.items():
            d = datetime.strptime(ds, "%Y-%m-%d").date()
            candles.append(Candle(
                symbol=symbol,
                date=d,
                open=float(entry["1. open"]),
                high=float(entry["2. high"]),
                low=float(entry["3. low"]),
                close=float(entry["4. close"]),
                volume=int(entry["5. volume"]),
            ))
        candles.sort(key=lambda c: c.date)
        return candles

class IntradayParser:
    @staticmethod
    def parse_intraday_timeseries(data: Dict, symbol: str) -> List[IntradayCandle]:
        block = next((v for k, v in data.items() if "Time Series" in k), {})
        candles: List[IntradayCandle] = []
        for ts_local, entry in block.items():
            dt_local = datetime.strptime(ts_local, "%Y-%m-%d %H:%M:%S").replace(tzinfo=EXCHANGE_TZ)
            ts_utc = dt_local.astimezone(timezone.utc)
            candles.append(IntradayCandle(
                symbol=symbol,
                ts=ts_utc,
                open=float(entry["1. open"]),
                high=float(entry["2. high"]),
                low=float(entry["3. low"]),
                close=float(entry["4. close"]),
                volume=int(entry["5. volume"]),
            ))
        candles.sort(key=lambda c: c.ts)
        return candles
