"""Centralized logging configuration."""

import logging
import sys
import time


def get_logger(name: str) -> logging.Logger:
    """Create and return a configured logger instance."""
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S UTC"
        )
        formatter.converter = time.gmtime  # Use UTC
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
