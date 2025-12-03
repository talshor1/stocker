# main.py
#!/usr/bin/env python3
from __future__ import annotations

import signal
import sys
import threading

import uvicorn

from config.config_parser import parse_args
from config.config import ConfigLoader
from db.task_db import init_task_repo
from logger.logger import setup_logging, get_logger
from worker.workers import Workers


def run_api_server():
    """Runs the FastAPI server using uvicorn."""
    uvicorn.run("server.app:app", host="0.0.0.0", port=8000, log_level="info", reload=False)

def main():
    args = parse_args()
    logger.info("Received args: %s", vars(args))

    cfg = ConfigLoader.load(args.config)
    logger.info("Loaded config: %s", cfg)

    init_task_repo(cfg.mongo)

    workers = Workers(
        cfg = cfg,
        max_workers = 1,
        max_wait_time = 10
    )

    logger.info("Starting workers loop")
    workers_thread = threading.Thread(
        target =workers.start_loop,
        name = "Workers",
        daemon = False
    )

    logger.info("Starting API Server")
    api_thread = threading.Thread(
        target=run_api_server,
        name="ApiServer",
        daemon=False
    )

    def signal_handler(sig, frame):
        logger.info("Received shutdown signal, stopping...")
        workers.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    workers_thread.start()
    api_thread.start()

    try:
        workers_thread.join()
        api_thread.join()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        workers.stop()
        sys.exit(0)

if __name__ == "__main__":
    setup_logging()
    logger = get_logger(__name__)
    main()
