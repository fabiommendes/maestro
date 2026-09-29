from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from pydantic import Field, model_validator


class Submission(ABC):
    """Base class for all submissions."""

    # Variants
    Dir: ClassVar[Dir]
    GhClassroom: ClassVar[type[GhClassroom]]
    GhClassroomMultiRepo: ClassVar[type[GhClassroomMultiRepo]]

    # Expected attributes
    tmp_paths: tuple[Path, ...] = Field(default_factory=tuple)


@Submission.register
@dataclass
class Dir:
    path: Path = Path("submissions")
    tmp_paths = ()


@Submission.register
@dataclass
class GhClassroomMultiRepo:
    repo_ids: list[str] = Field(min_length=1)


@Submission.register
@dataclass
class GhClassroom:
    repo_id: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_repo_ids(self) -> GhClassroom:
        if not self.repo_id.isdigit():
            raise ValueError("repo_id must be a string of digits")
        return self


Submission.Dir = Dir()
Submission.GhClassroom = GhClassroom
Submission.GhClassroomMultiRepo = GhClassroomMultiRepo
