# main.py
#!/usr/bin/env python3
from __future__ import annotations

from config.config_parser import parse_args
from config.config import ConfigLoader
from logger.logger import setup_logging, get_logger
from scheduler.scheduler import schedule

def main():
    args = parse_args()
    logger.info("Received args: %s", vars(args))

    cfg = ConfigLoader.load(args.config)
    logger.info("Loaded config: %s", cfg)

    schedule(cfg)

if __name__ == "__main__":
    setup_logging()
    logger = get_logger(__name__)
    main()
