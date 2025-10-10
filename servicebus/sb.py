from __future__ import annotations
import json
from dataclasses import asdict
from datetime import datetime
from logging import getLogger

from azure.servicebus import ServiceBusClient, ServiceBusMessage
from models.task import Task
from models.ServiceBusSettings import ServiceBusSettings

logger = getLogger(__name__)

def _json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

def send_task(sb: ServiceBusSettings, task: Task) -> None:
    logger.info("Trying to send task to service bus")
    payload = asdict(task)

    sbc = ServiceBusClient.from_connection_string(sb.url)
    with sbc:
        sender = sbc.get_queue_sender(queue_name=sb.queue)
        with sender:
            msg = ServiceBusMessage(
                json.dumps(payload, default=_json_serializer),
                content_type="application/json"
            )
            sender.send_messages(msg)
