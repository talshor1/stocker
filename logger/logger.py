import logging
import sys

def setup_logging(level=logging.INFO):
    # Prevent duplicate handlers if called multiple times
    if logging.getLogger().handlers:
        return

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(handler)

def get_logger(name: str = __name__) -> logging.Logger:
    return logging.getLogger(name)
