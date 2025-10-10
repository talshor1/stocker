from dataclasses import dataclass
from models import MongoSettings
from models.ServiceBusSettings import ServiceBusSettings

@dataclass(frozen=True)
class AppConfig:
    api_key: str
    function: str
    base_url: str
    mongo: MongoSettings
    sb: ServiceBusSettings