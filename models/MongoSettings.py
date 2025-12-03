from dataclasses import dataclass

@dataclass(frozen=True)
class MongoSettings:
    uri: str | None
    db: str
    candles_collection: str
    tasks_collection: str