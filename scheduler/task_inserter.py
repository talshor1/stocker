from __future__ import annotations

import certifi
from pymongo.collection import Collection
from datetime import datetime
from logging import getLogger

from pymongo import MongoClient

from models import MongoSettings
from models.task import Task

logger = getLogger(__name__)

def _json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

def insert_task(ms: MongoSettings, task: Task) -> None:
    logger.info("Trying to send task to db")

    mc = MongoClient(
        ms.uri,
        serverSelectionTimeoutMS=60000,
        connectTimeoutMS=30000,
        socketTimeoutMS=30000,
        tlsCAFile=certifi.where(),
    )
    tasks: Collection = mc[ms.db][ms.tasks_collection]

    task_dict = {
        "taskId": task.taskId,
        "op": task.op,
        "symbol": task.symbol,
        "status": task.status,
        "created_at": task.created_at,
    }

    result = tasks.insert_one(task_dict)
    logger.info("Task %s inserted with id: %s", task.taskId, result.inserted_id)

    mc.close()