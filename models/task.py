from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
import uuid

@dataclass(frozen = True)
class Task:
    taskId: str
    symbol: str
    days_back: int = 5
    minute_interval: int = 1
    is_enabled: bool = True
    task_interval: int = 0
    op: str = "fetch_and_save_intraday"
    created_at: datetime = datetime.now(timezone.utc)

    @staticmethod
    def new(symbol: str,
        days_back: int = 5,
        minute_interval: int = 1,
        op: str = "fetch_and_save_intraday") -> Task:
        return Task(
            taskId = str(uuid.uuid4()),
            symbol = symbol,
            days_back = days_back,
            minute_interval = minute_interval,
            task_interval = 0,
            op = op,
            created_at=datetime.now(timezone.utc)
        )
