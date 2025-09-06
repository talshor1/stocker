from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime

@dataclass(frozen=True)
class Candle:
    symbol: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int

@dataclass(frozen=True)
class IntradayCandle:
    symbol: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
