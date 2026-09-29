from __future__ import annotations

from abc import ABC
from contextlib import contextmanager
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
)

import rich
from pydantic import Field, model_validator

from ..calltree import CallTree
from ..model import Model

if TYPE_CHECKING:
    from ..classroom import Classroom

    NOT_GIVEN = NotImplemented
else:
    NOT_GIVEN = object()


class BaseActivity(Model, ABC):
    """
    Base class for all activities.

    Properties:
        type:
            The type of the activity, e.g., "repository", "spreadsheet",
            "text-file", etc.
        name:
            A human readable name for the activity.
        path:
            The path to the activity directory.
        classroom:
            The classroom this activity belongs to.
    """

    CONFIG_FILE: ClassVar[str] = "activity.yaml"
    type: str
    name: str
    path: Path = Field(exclude=True, repr=False)
    classroom: Classroom = Field(exclude=True, repr=False)
    silent: bool = Field(default=False, exclude=True, repr=False)

    @property
    def config_path(self) -> Path:
        return self.path / self.CONFIG_FILE

    @property
    def slug(self) -> str:
        return self.path.name

    @model_validator(mode="after")
    def _validate_path(self) -> BaseActivity:
        if not self.path.is_absolute():
            raise ValueError(f"Activity path must be absolute: {self.path!r}")
        if not self.path.exists():
            raise ValueError(f"Activity path does not exist: {self.path!r}")
        return self

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._store: dict[str, Any] = {}

    def pipeline(self) -> CallTree:
        """
        Return the initial actions to be performed in the pipeline.
        """
        raise NotImplementedError

    def run(self):
        """
        Run the task pipeline for activity.
        """
        pipeline = self.pipeline()
        result = pipeline.begin()
        return result

    @contextmanager
    def silence_notifications(self):
        """
        Context manager to silence notifications during the execution of a block of code.
        """
        previous_silent = self.silent
        self.silent = True
        try:
            yield
        finally:
            self.silent = previous_silent

    def notify(
        self,
        message: str,
        *,
        title="",
        mode: Literal["error", "info", "warning"] = "info",
    ) -> None:
        """
        Notify the user about the current step in the pipeline.
        """
        if self.silent:
            return
        color_map = {"error": "red", "info": "", "warning": "yellow"}
        color = color_map.get(mode, "")

        if not title:
            title = mode.title()
        lines = message.splitlines()
        if len(lines) > 1:
            rich.print(f"[b {color}]{title}[/]")
            for line in message.splitlines():
                rich.print(f"  [gray]{line}[/]")
            rich.print()
        else:
            rich.print(f"[b {color}]{title}:[/] [gray]{message}[/]")


class Spreadsheet(BaseActivity):
    """
    Each activity is a row in a spreadsheet.
    """

    type: Literal["spreadsheet"] = "spreadsheet"


class TextFile(BaseActivity):
    """
    Each activity is single text file.
    """

    type: Literal["text-file"] = "text-file"


def __getattr__(name: str) -> Any:
    if name == "Classroom":
        from ..classroom import Classroom

        return Classroom

    raise AttributeError(name)
