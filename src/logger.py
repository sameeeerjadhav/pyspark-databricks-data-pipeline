"""Application logging for the ShopSphere pipeline."""

from __future__ import annotations

import logging
import os
from pathlib import Path


def get_logger(name: str = "shopsphere") -> logging.Logger:
    """Return a configured logger. Child loggers propagate to shopsphere."""
    parent = logging.getLogger("shopsphere")
    if not parent.handlers:
        parent.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s %(levelname)s - %(message)s")
        stream = logging.StreamHandler()
        stream.setFormatter(formatter)
        parent.addHandler(stream)

        log_root = Path(os.environ.get("PIPELINE_BASE_PATH", Path(__file__).resolve().parents[1]))
        log_dir = log_root / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_dir / "pipeline.log", encoding="utf-8")
        file_handler.setFormatter(formatter)
        parent.addHandler(file_handler)
        parent.propagate = False
    if name == "shopsphere":
        return parent
    return logging.getLogger(name)
