# workers.py
from __future__ import annotations

import time
import traceback

from concurrent.futures import ThreadPoolExecutor, Future
from logging import getLogger
from typing import Any, Callable, Dict
import threading

from db.task_db import get_task_repo
from models import AppConfig
from models.task import Task
from tasks.data_fetcher import fetch_and_save_intraday
from tasks.run_prompt import run_prompt

logger = getLogger(__name__)

DISPATCH: Dict[str, Callable[[Any, str], None]] = {
    "fetch_and_save_intraday": lambda cfg, task: fetch_and_save_intraday(
        cfg,
        symbol = task.symbol,
        outfile = task.file_name,
    ),
    "run_prompt": lambda cfg, task: run_prompt(cfg, task)
}

class Workers:
    def __init__(
            self,
            cfg: AppConfig,
            *,
            max_workers: int = 1,
            max_wait_time: int = 10,
    ):
        self.cfg = cfg
        self.max_workers = max_workers
        self.max_wait_time = max_wait_time
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self.running_tasks: Dict[str, Future] = {}
        self._lock = threading.Lock()
        self._running = True

        logger.info("Workers initialized: max_workers=%d",max_workers)

    def start_loop(self) -> None:
        logger.info("Workers loop starting (max_workers=%d)", self.max_workers)

        try:
            while self._running:
                try:
                    repo = get_task_repo()
                    task = repo.get_one_task()
                    if not task:
                        logger.info("No tasks found, waiting...")
                        time.sleep(self.max_wait_time)
                        continue
                    else:
                        logger.info("Submitting task %s", task.taskId)
                        self._submit_task(task)
                        logger.info("Submitted task %s", task.taskId)
                except KeyboardInterrupt:
                    logger.info("Received interrupt signal, shutting down...")
                    self._running = False
                    break
                except Exception as e:
                    logger.error("Error in worker loop: %s\n%s", e, traceback.format_exc())
                    time.sleep(self.max_wait_time)

        finally:
            self._shutdown()

    def _submit_task(self, task: Task) -> None:
        try:
            logger.info("Received task: id=%s, symbol=%s, stage=%s",task.taskId, task.symbol, task.stage)
            if task.stage == 0:
                fn = DISPATCH.get("fetch_and_save_intraday")
            if task.stage == 1:
                fn = DISPATCH.get("run_prompt")

            def run_task():
                try:
                    logger.info("Executing task %s", task.taskId)
                    fn(self.cfg, task)
                    logger.info("Task %s executed successfully", task.taskId)

                    task_repo = get_task_repo()
                    if task.stage == 0:
                        logger.info("Moving task %s to stage 1", task.taskId)
                        task_repo.update_task_status_and_stage(task.taskId, "pending", 1)
                    else:
                        logger.info("Marking task %s as completed", task.taskId)
                        task_repo.update_task_status(task.taskId, "completed")
                except Exception as e:
                    logger.error("Task %s failed: %s\n%s", task.taskId, e, traceback.format_exc())

            self._executor.submit(run_task)


        except Exception as e:
            logger.error("Error submitting task: %s\n%s", e, traceback.format_exc())

    def _shutdown(self) -> None:
        logger.info("Shutting down worker pool...")
        self._running = False

        logger.info("Waiting for %d in-flight tasks to complete...", len(self.running_tasks))
        self._executor.shutdown(wait=True)

        logger.info("Workers stopped.")

    def stop(self) -> None:
        logger.info("Stop requested")
        self._running = False