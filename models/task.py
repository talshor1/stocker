from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

def _outfile_from_task(task_id: str, task_symbol:str, base_dir: str = "DATA") -> str:
    sym = task_symbol
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    p = Path(base_dir) / sym / day / task_id
    p.mkdir(parents=True, exist_ok=True)
    return str(p / f"data.csv")

@dataclass(frozen = True)
class Task:
    taskId: str
    symbol: str
    minute_interval: int = 1,
    file_name: str = ""
    status: str = "submitted"
    op: str = "fetch_and_save_intraday"
    created_at: datetime = datetime.now(timezone.utc)
    stage: int = 0  # 0 - bring data, 1 - make prompt

    @staticmethod
    def new(symbol: str,
        minute_interval: int = 1,
        op: str = "fetch_and_save_intraday",
        file_name: str = "",
        task_id: str = "",
        status: str = "submitted") -> Task:
        return Task(
            taskId = task_id,
            symbol = symbol,
            minute_interval = minute_interval,
            file_name = _outfile_from_task(task_id, symbol),
            status = status,
            op = op,
            created_at=datetime.now(timezone.utc),
            stage = 0
        )
