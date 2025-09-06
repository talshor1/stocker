from dataclasses import dataclass
from models import MongoSettings

@dataclass(frozen=True)
class AppConfig:
    api_key: str
    function: str
    base_url: str
    symbol: str
    days: int
    intraday_minutes: int
    mongo: MongoSettings