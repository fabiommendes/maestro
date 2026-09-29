from __future__ import annotations

from functools import cache
from typing import TYPE_CHECKING

from .base import BaseActivity, Spreadsheet, TextFile
from .repository import Repository

if TYPE_CHECKING:
    from ..classroom import Classroom

type Activity = Repository | Spreadsheet | TextFile

__all__ = [
    "BaseActivity",
    "Repository",
    "TextFile",
    "Spreadsheet",
    "Activity",
]


def load_activity(data: dict, classroom: Classroom, **kwargs) -> Activity:
    """
    Load an activity from a dictionary, string, or file path.

    Args:
        data: The activity data to load.
        **kwargs: Additional keyword arguments for the model.

    Returns:
        An instance of a concrete activity type.
    """
    model_rebuild()

    data = data.copy()
    data["classroom"] = classroom
    activity: Activity
    match data["type"]:
        case "repository":
            activity = Repository.model_validate(data, **kwargs)
        case "spreadsheet":
            activity = Spreadsheet.model_validate(data, **kwargs)
        case "text-file":
            activity = TextFile.model_validate(data, **kwargs)
        case _:
            raise ValueError(f"Unknown activity type: {data['type']}")

    return activity


@cache
def model_rebuild() -> None:
    global Classroom

    from ..classroom import Classroom
    from . import base, repository

    base.Classroom = Classroom  # type: ignore
    repository.Classroom = Classroom  # type: ignore

    ns = globals()
    for cls_name in __all__:
        cls = ns.get(cls_name)
        if isinstance(cls, type) and issubclass(cls, BaseActivity):
            rebuilt = cls.model_rebuild(raise_errors=False)
            if not rebuilt:
                raise RuntimeError(f"Failed to rebuild model for {cls_name}")
