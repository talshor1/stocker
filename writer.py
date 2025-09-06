# writer.py
from __future__ import annotations
from typing import Iterable, Any, Dict
import csv
from datetime import datetime
from dataclasses import is_dataclass, asdict

class CsvWriter:
    @staticmethod
    def write_intraday(path: str, candles: Iterable[Any]) -> None:
        def _to_dict(item: Any) -> Dict[str, Any]:
            if isinstance(item, dict):
                return item
            if is_dataclass(item):
                return asdict(item)
            return {
                "symbol": getattr(item, "symbol"),
                "ts": getattr(item, "ts"),
                "open": getattr(item, "open"),
                "high": getattr(item, "high"),
                "low": getattr(item, "low"),
                "close": getattr(item, "close"),
                "volume": getattr(item, "volume"),
            }

        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["symbol", "timestamp", "open", "high", "low", "close", "volume"])
            for item in candles:
                d = _to_dict(item)
                ts = d["ts"]
                if isinstance(ts, datetime):
                    ts_str = ts.isoformat().replace("+00:00", "Z")
                else:
                    try:
                        ts_dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                        ts_str = ts_dt.isoformat().replace("+00:00", "Z")
                    except Exception:
                        ts_str = str(ts)

                w.writerow([
                    d["symbol"], ts_str, d["open"], d["high"], d["low"], d["close"], d["volume"]
                ])
