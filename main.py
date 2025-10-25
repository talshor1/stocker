# main.py
#!/usr/bin/env python3
from __future__ import annotations

import signal
import sys
import threading

from config.config_parser import parse_args
from config.config import ConfigLoader
from logger.logger import setup_logging, get_logger
from scheduler.scheduler import schedule
from worker.workers import Workers

def main():
    args = parse_args()
    logger.info("Received args: %s", vars(args))

    cfg = ConfigLoader.load(args.config)
    logger.info("Loaded config: %s", cfg)

    workers = Workers(
        cfg = cfg,
        max_workers = 4,
        poll_interval = 1.0,
        max_wait_time = 5.0
    )

    logger.info("Starting workers loop")
    workers_thread = threading.Thread(
        target =workers.start_loop,
        name = "Workers",
        daemon = False
    )
    workers_thread.start()

    logger.info("Starting scheduler")
    scheduler_thread = threading.Thread(
        target = schedule,
        args = (cfg,),
        name = "SchedulerThread",
        daemon = False
    )
    scheduler_thread.start()

    def signal_handler(sig, frame):
        logger.info("Received shutdown signal, stopping...")
        workers.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        workers_thread.join()
        scheduler_thread.join()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        workers.stop()
        sys.exit(0)

if __name__ == "__main__":
    setup_logging()
    logger = get_logger(__name__)
    main()
