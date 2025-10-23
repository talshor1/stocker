from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class MongoSettings:
    uri: Optional[str]
    db: str
    candles_collection: str
    ctx_collection: str
    tasks_collection: str
