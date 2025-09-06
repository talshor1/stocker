from __future__ import annotations
from typing import Iterable, Dict, Any, Iterator
from pathlib import Path
import csv
from datetime import datetime, date, timezone

def docs_from_csv(csv_path: str, default_symbol: str) -> Iterator[Dict[str, Any]]:
    p = Path(csv_path)
    if not p.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    with p.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise SystemExit("CSV has no header row.")

        headers = [h.strip().lower() for h in reader.fieldnames]
        header_set = set(headers)

        has_symbol = "symbol" in header_set
        has_date = "date" in header_set
        has_ts = "timestamp" in header_set
        required_ohlc = {"open", "high", "low", "close", "volume"}

        if not required_ohlc.issubset(header_set) or not (has_date or has_ts or has_symbol):
            raise SystemExit(
                "CSV must include open,high,low,close,volume and either 'date' (daily) or 'timestamp' (intraday)."
            )

        for raw in reader:
            row = { (k or "").strip().lower(): (v.strip() if isinstance(v, str) else v)
                    for k, v in raw.items() }

            symbol = (row.get("symbol") or default_symbol).strip()

            if has_ts:
                ts_str = row["timestamp"]
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                except Exception:
                    ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
            else:
                d: date = date.fromisoformat(row["date"])
                ts = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)

            vol_raw = row["volume"]
            try:
                volume = int(vol_raw)
            except ValueError:
                volume = int(float(vol_raw))

            yield {
                "symbol": symbol,
                "ts": ts,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": volume,
            }
