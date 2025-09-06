# main.py
#!/usr/bin/env python3
from __future__ import annotations

from config.config_parser import parse_args
from config.config import ConfigLoader
from scheduler.scheduler import schedule

def main():
    args = parse_args()
    cfg = ConfigLoader.load(args.config)

    schedule(cfg)

if __name__ == "__main__":
    main()
