# runners.py
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, Future
from datetime import datetime, timezone, timedelta
from logging import getLogger
from typing import Any, Callable, Dict, Optional
import os, socket, threading, time, traceback

from pymongo import MongoClient
from pymongo.collection import Collection

from models.task import TaskStatus

logger = getLogger(__name__)

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _mk_worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{threading.get_ident()}"

class Runners:
    def __init__(
        self,
        cfg: Any,
        dispatch: Dict[str, Callable[[Any, dict], None]],
        *,
        max_workers: int = 4,
        poll_interval: float = 1.5,
        collection_name: str = "tasks",
    ):
        self.cfg = cfg
        self.dispatch = dispatch
        self.max_workers = max_workers
        self.poll_interval = poll_interval
        uri = cfg.mongo.uri
        db_name = cfg.mongo.db
        coll_name = collection_name
        self._mc = MongoClient(uri)
        self.tasks: Collection = self._mc[db_name][coll_name]
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._inflight: Dict[str, Future] = {}
        self._lock = threading.Lock()

    def start_loop(self) -> None:
        logger.info("Runners loop starting (workers=%d)", self.max_workers)
        try:
            while True:
                task_doc = self._claim_one()
                if not task_doc:
                    time.sleep(self.poll_interval)
                    continue

                self._submit(task_doc)
        finally:
            self._shutdown()

    def _claim_one(self) -> Optional[dict]:
        now = now_utc()
        worker_id = _mk_worker_id()

        doc = self.tasks.find_one_and_update(
            filter={
                "status": TaskStatus.QUEUED,
            },
            update={
                "$set": {
                    "status": TaskStatus.RUNNING,
                    "started_at": now,
                    "leased_by": worker_id,
                },
                "$inc": {"attempts": 1},
            }
        )
        if doc:
            logger.info("Claimed task %s (op=%s, symbol=%s, attempts=%s)",
                        doc.get("taskId"), doc.get("op"), doc.get("symbol"), doc.get("attempts"))
        else:
            logger.info("No task available")
        return doc

    def _submit(self, task_doc: dict) -> None:
        op = task_doc.get("op")
        fn = self.dispatch.get(op)
        tid = task_doc.get("taskId")

        if not fn:
            logger.error("Unsupported op %r for task %s — marking failed", op, tid)
            self._fail_task(task_doc, "unsupported op")
            return

        def run_task():
            try:
                logger.info("Running task %s", tid)
                fn(self.cfg, task_doc)
                self._succeed_task(task_doc)
            except Exception as e:
                logger.error("Task %s failed: %s\n%s", tid, e, traceback.format_exc())
                self._fail_task(task_doc, str(e))
                raise

        fut = self._executor.submit(run_task)
        with self._lock:
            self._inflight[tid] = fut



    def _succeed_task(self, task_doc: dict) -> None:
        tid = task_doc["taskId"]
        self.tasks.update_one(
            {"taskId": tid, "status": TaskStatus.RUNNING},
            {"$set": {
                "status": TaskStatus.DONE,
                "finished_at": now_utc(),
                "error": None,
            }}
        )
        logger.info("Task %s -> %s", tid, TaskStatus.DONE)

    def _fail_task(self, task_doc: dict, error: str) -> None:
        tid = task_doc["taskId"]
        attempts = int(task_doc.get("attempts", 1))
        backoff_sec = min(2 ** attempts, 300)  # cap 5m
        self.tasks.update_one(
            {"taskId": tid, "status": TaskStatus.FAILED},
            {"$set": {
                "status": TaskStatus.FAILED,
                "finished_at": now_utc(),
                "error": error,
                "next_run_at": now_utc() + timedelta(seconds=backoff_sec),
            }}
        )
        logger.info("Task %s -> %s (retry in %ss)", tid, TaskStatus.FAILED, backoff_sec)

    def _shutdown(self) -> None:
        logger.info("Shutting down runner pool…")
        self._executor.shutdown(wait=True)
        try:
            self._mc.close()
        except Exception:
            pass
        logger.info("Runners stopped.")
