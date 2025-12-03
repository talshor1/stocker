import logging
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Optional, Any, Mapping

from pymongo import MongoClient
from pymongo.collection import Collection
import certifi

from models import MongoSettings
from models.task import Task

logger = logging.getLogger(__name__)

class TaskRepo:
    def __init__(self, cfg: MongoSettings):
        self.client = MongoClient(
            cfg.uri,
            appname="stocker-app-tasks",
            serverSelectionTimeoutMS=60000,
            connectTimeoutMS=30000,
            socketTimeoutMS=30000,
            tlsCAFile=certifi.where(),
        )
        self.db = self.client[cfg.db]
        col_name = getattr(cfg, 'tasks_collection', 'tasks')
        self.tasks_col: Collection = self.db[col_name]

    def close(self) -> None:
        self.client.close()

    @staticmethod
    def from_dic_to_task(task_doc: dict) -> Optional[Task]:
        try:
            return Task(**task_doc)
        except TypeError as e:
            logger.error(f"Failed to map document to Task: {e}")
            return None

    def insert_task(self, task: Task) -> str:
        logger.info(f"Inserting task: {task.taskId}")
        doc = asdict(task)
        if "created_at" not in doc:
            doc["created_at"] = datetime.now(timezone.utc)

        res = self.tasks_col.insert_one(doc)
        logger.info(f"Task inserted: {task.taskId}")
        return str(res.inserted_id)

    def get_task(self, task_id: str) -> Task | None:
        logger.info(f"Getting task status: {task_id}")
        doc = self.tasks_col.find_one({"taskId": task_id},projection={"_id": 0})
        if doc:
            return self.from_dic_to_task(doc)
        return None

    def update_task_status(self, task_id: str, status: str) -> None:
        logger.info(f"Updating task status: {task_id} -> {status}")
        self.tasks_col.update_one({"taskId": task_id}, {"$set": {"status": status}})

    def update_task_status_and_stage(self, task_id: str, status: str, stage: int) -> None:
        logger.info(f"Updating task status and stage: {task_id} -> {status}, {stage}")
        self.tasks_col.update_one({"taskId": task_id}, {"$set": {"status": status, "stage": stage}})

    def get_one_task(self) -> Optional[Task]:
        logger.info("Getting one task")
        task = self.tasks_col.find_one({"stage": 1, "status": "pending"}, projection={"_id": 0})
        if task:
            self.update_task_status(task["taskId"], "running")
            return self.from_dic_to_task(task)

        task = self.tasks_col.find_one({"status": "submitted"}, projection={"_id": 0})
        if task:
            self.update_task_status(task["taskId"], "running")
            return self.from_dic_to_task(task)

        logger.info("No tasks found")
        return None


_task_repo: Optional[TaskRepo] = None

def init_task_repo(cfg: MongoSettings) -> None:
    global _task_repo
    _task_repo = TaskRepo(cfg)

def get_task_repo() -> TaskRepo:
    global _task_repo
    return _task_repo