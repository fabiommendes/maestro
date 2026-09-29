from pathlib import Path
from typing import Optional, Any
from dataclasses import dataclass
import pandas as pd

from .base import Model, StudentId
from ..loaders import read_file
from ..utils import (
    url_to_username_normalizer,
    GITHUB_USER_URL,
    CHEKIO_USER_URL,
    BEECROWD_USER_URL,
)


__all__ = ["Student"]


@dataclass
class Student(Model):
    """
    Basic student representation.

    Requires a unique id per classroom.
    """

    id: str
    name: str
    email: Optional[str] = None
    github_id: Optional[str] = None
    checkio_id: Optional[str] = None
    beecrowd_id: Optional[str] = None
    # ... probably we need many more, and a more robust way to create new accounts
    is_active: bool = True

    class Inactive(KeyError):
        """
        Raised when expecting an active student and only an active one was found.
        """

    _EXTERNAL_ACCOUNTS = {
        "email": "E-mail",
        "github_id": "Github",
        "checkio_id": "Checkio",
        "beecrowd_id": "Beecrowd",
    }
    _NORMALIZERS = {
        "id": lambda x: normalize_student_id(x),
        "name": lambda x: x,
        "email": lambda x: x or None,
        "github_id": url_to_username_normalizer(GITHUB_USER_URL),
        "checkio_id": url_to_username_normalizer(CHEKIO_USER_URL),
        "checkio_id": url_to_username_normalizer(BEECROWD_USER_URL),
    }

    @property
    def slug(self):
        "An id that is safe to use as a part of a filename."
        return self.id.replace("/", "_").replace("\\", "_")


def normalize_student_id(x: Any) -> StudentId:
    if isinstance(x, float):
        if x == int(x):
            return normalize_student_id(int(x))
        else:
            return str(x)
    elif isinstance(x, Student):
        return x.id
    elif x is None:
        return ""
    return str(x)


def parse_roster_file(path: Path) -> pd.DataFrame:
    """
    Read raw dataframe with raw roster file data.
    """
    data = read_file(path).fillna("").astype({"github_id": str, "checkio_id": str})
    data["id"] = data["id"].transform(normalize_student_id)  # type: ignore
    if "is_active" in data:
        data["is_active"] = data["is_active"].astype(str).str.lower() == "true"
    return data
