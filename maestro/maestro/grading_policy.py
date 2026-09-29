from __future__ import annotations

import enum
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

from .types import QuestionId


class GradingPolicy:
    """
    Represents a policy reacting to duplicate submissions.
    """

    class Kind(enum.IntEnum):
        """
        Kind of the policy.
        """

        IGNORE = 0
        MULTIPLICATIVE_FACTOR = 2
        ADDITIVE_FACTOR = 3

    kind: Kind
    _question_id: QuestionId | None = None
    _file: Path | None = None
    _factor: float | None = None

    @classmethod
    def _kwargs(cls, question_or_file: QuestionId | Path | None = None):
        if isinstance(question_or_file, Path):
            return {"_file": question_or_file, "_raise": False}
        elif question_or_file is None:
            return {"_raise": False}
        else:
            return {"_question_id": question_or_file, "_raise": False}

    @classmethod
    def ignore(cls, question_or_file: QuestionId | Path | None = None) -> GradingPolicy:
        """
        Do not change grades.

        This is useful, for instance, to ignore duplicates when they are found.
        """
        return cls(cls.Kind.IGNORE, **cls._kwargs(question_or_file))  # type: ignore

    @classmethod
    def nulify(cls, question_or_file: QuestionId | Path | None = None) -> GradingPolicy:
        """
        Create a policy that ignores duplicates.
        """
        return cls(
            cls.Kind.MULTIPLICATIVE_FACTOR,
            _discount=0.0,
            **cls._kwargs(question_or_file),
        )  # type: ignore

    def __init__(
        self,
        kind: Kind,
        _question_id: QuestionId | None = None,
        _file: Path | None = None,
        _discount: float | None = None,
        _raise: bool = True,
    ):
        if _raise:
            msg = "DuplicatePolicy is an abstract class and cannot be instantiated directly."
            raise ValueError(msg)
        self.kind = kind
        self._factor = _discount
        self._question_id = _question_id
        self._file = _file

    def to_pydantic(self):
        """
        Convert the policy to a Pydantic model.
        """
        match self.kind:
            case self.Kind.IGNORE:
                return Ignore()
            case self.Kind.MULTIPLICATIVE_FACTOR:
                return MultiplicativeFactor(
                    question_id=self._question_id,
                    file=self._file,
                    factor=self._factor,
                )
            case self.Kind.ADDITIVE_FACTOR:
                return AdditiveFactor(
                    question_id=self._question_id,
                    file=self._file,
                    factor=self._factor,
                )


#
# Pydantic models
#
class Ignore(BaseModel):
    """
    Ignore duplicates.
    """

    type: Literal["ignore"] = "ignore"
