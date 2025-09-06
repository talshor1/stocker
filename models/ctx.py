# models/ctx_model.py
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional, Literal, Dict, Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Ctx:
    symbol: str
    days_back: int = 5
    minute_interval: int = 1
    is_enabled: bool = True
    task_interval: int = 0
    type: Literal["fetch_intraday"] = "fetch_intraday"
    timezone: str = "UTC"
    last_run: Optional[datetime] = None
    created_at: datetime = field(default_factory=utc_now)
    op: Literal["fetch_and_save_intraday"] = "fetch_and_save_intraday"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def build_ctx(symbol: str, task_interval_minutes: int) -> Dict[str, Any]:
    ctx = Ctx(symbol=symbol, task_interval=task_interval_minutes)
    return ctx.to_dict()
