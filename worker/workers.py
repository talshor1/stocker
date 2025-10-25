# workers.py
from __future__ import annotations

import time
import traceback

import json
from concurrent.futures import ThreadPoolExecutor, Future
from logging import getLogger
from typing import Any, Callable, Dict
import threading


from azure.servicebus import ServiceBusClient, ServiceBusReceiver, ServiceBusMessage

from models import AppConfig
from scheduler.scheduler import DISPATCH

logger = getLogger(__name__)


class Workers:
    def __init__(
            self,
            cfg: AppConfig,
            *,
            max_workers: int = 4,
            poll_interval: float = 1.0,
            max_wait_time: float = 5.0,
    ):
        self.cfg = cfg
        self.max_workers = max_workers
        self.poll_interval = poll_interval
        self.max_wait_time = max_wait_time

        self._sb_client = ServiceBusClient.from_connection_string(
            conn_str=cfg.sb.url,
            logging_enable=True
        )
        self._receiver: ServiceBusReceiver = self._sb_client.get_queue_receiver(
            queue_name=cfg.sb.queue
        )

        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self.running_tasks: Dict[str, Future] = {}
        self._lock = threading.Lock()
        self._running = True

        logger.info(
            "Workers initialized: max_workers=%d, queue=%s, poll_interval=%.2fs",
            max_workers, cfg.sb.queue, poll_interval
        )

    def start_loop(self) -> None:
        logger.info("Workers loop starting (max_workers=%d)", self.max_workers)

        try:
            while self._running:
                try:
                    messages = self._receiver.receive_messages(
                        max_wait_time=self.max_wait_time,
                        max_message_count=self.max_workers
                    )

                    if not messages:
                        logger.debug("No tasks available, waiting...")
                        time.sleep(self.poll_interval)
                        continue

                    for message in messages:
                        self._submit_task(message)

                except KeyboardInterrupt:
                    logger.info("Received interrupt signal, shutting down...")
                    self._running = False
                    break
                except Exception as e:
                    logger.error("Error in worker loop: %s\n%s", e, traceback.format_exc())
                    time.sleep(self.poll_interval)

        finally:
            self._shutdown()

    def _submit_task(self, message: ServiceBusMessage) -> None:
        try:
            body_bytes = b''.join(message.body)
            task_doc = json.loads(body_bytes.decode('utf-8'))

            task_id = task_doc.get("taskId", str(message.message_id))
            op = task_doc.get("op")

            if not op or not task_id:
                logger.error("Invalid task: missing op or taskId")
                self._receiver.abandon_message(message)
                return

            logger.info(
                "Received valid task: id=%s, op=%s, symbol=%s",
                task_id, op, task_doc.get("symbol")
            )

            fn = DISPATCH.get(op)
            if not fn:
                logger.error("Unsupported operation %r for task %s", op, task_id)
                self._receiver.abandon_message(message)
                return

            def run_task():
                try:
                    logger.info("Executing task %s", task_id)
                    fn(self.cfg, task_doc)
                    self._receiver.complete_message(message)
                    logger.info("Task %s completed successfully", task_id)

                except Exception as e:
                    logger.error("Task %s failed: %s\n%s", task_id, e, traceback.format_exc())
                    self._receiver.abandon_message(message)
                    logger.info("Task %s abandoned for retry", task_id)

                finally:
                    logger.info("Task %s finished", task_id)
                    with self._lock:
                        self.running_tasks.pop(task_id, None)

            fut = self._executor.submit(run_task)
            with self._lock:
                self.running_tasks[task_id] = fut

        except Exception as e:
            logger.error("Error submitting task: %s\n%s", e, traceback.format_exc())
            try:
                self._receiver.abandon_message(message)
            except Exception as abandon_error:
                logger.error("Failed to abandon message: %s", abandon_error)

    def _shutdown(self) -> None:
        logger.info("Shutting down worker pool...")
        self._running = False

        logger.info("Waiting for %d in-flight tasks to complete...", len(self.running_tasks))
        self._executor.shutdown(wait=True)

        try:
            self._receiver.close()
            self._sb_client.close()
            logger.info("Service Bus connections closed")
        except Exception as e:
            logger.error("Error closing Service Bus connections: %s", e)

        logger.info("Workers stopped.")

    def stop(self) -> None:
        logger.info("Stop requested")
        self._running = False