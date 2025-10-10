from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class ServiceBusSettings:
    url: str
    queue: str
