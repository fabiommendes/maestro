from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    """
    Global maestro configurations.
    """

    path: Path = Path("./maestro.yaml")
    is_saved: bool = False
