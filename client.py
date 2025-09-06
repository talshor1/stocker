from __future__ import annotations
from typing import Protocol, Dict, Any, Optional
import time
import requests

class MarketDataClient(Protocol):
    def fetch_series(self, function: str, **extra_params: Any) -> Dict[str, Any]: ...
    def fetch_time_series_daily(self, symbol: str, outputsize: str = "full") -> Dict[str, Any]: ...
    def fetch_time_series_intraday(self, symbol: str, interval: str = "5min",
                                   outputsize: str = "full", month: Optional[str] = None) -> Dict[str, Any]: ...


class AlphaVantageClient(MarketDataClient):

    VALID_INTRADAY_INTERVALS = {"1min", "5min", "15min", "30min", "60min"}

    def __init__(self, base_url: str, api_key: str, max_retries: int = 6, backoff_sec: int = 15):
        self.base_url = base_url
        self.api_key = api_key
        self.max_retries = max_retries
        self.backoff_sec = backoff_sec
        self.session = requests.Session()

    def fetch_time_series_daily(self, symbol: str, outputsize: str = "full") -> Dict[str, Any]:
        return self.fetch_series(
            function = "TIME_SERIES_DAILY",
            symbol = symbol,
            outputsize = outputsize,
            datatype = "json",
        )

    def fetch_time_series_intraday(
        self,
        symbol: str,
        interval: str = "5min",
        outputsize: str = "full",
        month: Optional[str] = None,
    ) -> Dict[str, Any]:
        if interval not in self.VALID_INTRADAY_INTERVALS:
            raise ValueError(f"interval must be one of {sorted(self.VALID_INTRADAY_INTERVALS)}")

        params: Dict[str, Any] = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": interval,
            "outputsize": outputsize,
            "datatype": "json",
        }
        if month:
            params["month"] = month

        return self.fetch_series(**params)

    def fetch_series(self, function: str, **extra_params: Any) -> Dict[str, Any]:
        params = {
            "function": function,
            "apikey": self.api_key,
            **extra_params,
        }
        return self._get_json_with_retries(params)

    # -------------------------- Internal helpers -----------------------------

    def _get_json_with_retries(self, params: Dict[str, Any]) -> Dict[str, Any]:
        attempt = 0
        while True:
            attempt += 1
            resp = self.session.get(self.base_url, params=params, timeout=30)
            if resp.status_code != 200:
                if attempt >= self.max_retries:
                    snippet = resp.text[:200].replace("\n", " ")
                    raise RuntimeError(f"HTTP {resp.status_code}: {snippet}")
                time.sleep(self.backoff_sec)
                continue

            data = resp.json()

            if "Error Message" in data:
                raise ValueError(f"Alpha Vantage error: {data['Error Message']}")

            info = data.get("Information")
            if info and "premium" in info.lower():
                raise RuntimeError(
                    "Alpha Vantage says this is a premium endpoint. "
                    "Use a non-premium function or upgrade your plan."
                )

            # Rate limit / throttle message comes in 'Note'
            note = data.get("Note")
            if note:
                if attempt >= self.max_retries:
                    raise RuntimeError(f"Rate limit hit repeatedly: {note}")
                time.sleep(self.backoff_sec)
                continue

            return data

    def close(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass
