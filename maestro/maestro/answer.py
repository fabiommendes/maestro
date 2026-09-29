from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from fractions import Fraction
from pathlib import Path
from typing import Any, Callable, Iterable

from .utils import load_entry_point


@dataclass
class Answer[Id = str]:
    """
    A question in the activity.

    It is used to define a question that requires manual grading.
    """

    id: Id
    _items: dict[str, Item]

    @classmethod
    def from_text(cls, id: Id, text: str) -> Answer[Id]:
        """
        Parse a question from a text.

        Args:
            name:
                A question identifier.
            text:
                The text to parse.

        Returns:
            A Question object.
        """
        return cls(id, {"body": TextItem(text=text)})

    @classmethod
    def from_file(cls, id: Id, path: Path) -> Answer[Id]:
        """
        Parse a question from a file.

        Args:
            name:
                A question identifier.
        """
        return cls.from_text(id, path.read_text(encoding="utf-8"))

    @classmethod
    def from_user_data(cls, id: Id, data: Any) -> Answer[Id]:
        """
        Uses the output of a user-defined parser to create a Question.

        This is a very lenient question constructor since we want to be flexible
        with the kind of data the user might provide.
        """
        if isinstance(data, str):
            return cls.from_text(id, data)

        return cls(id, {k: TextItem(text=v) for k, v in data.items()})

    def items(self) -> Iterable[tuple[str, Item]]:
        """
        Iterate over the items in the question.

        Returns:
            An iterable of (key, Item) pairs.
        """
        return self._items.items()


class Item(ABC):
    """
    Base class for question types.
    """

    weight: Fraction

    def is_invalid(self) -> bool:
        """
        Invalid submissions are not graded and do not enter in the plagiarism
        detection.
        """
        return False


@dataclass
class TextItem(Item):
    text: str
    weight: Fraction = Fraction(1)

    def is_invalid(self) -> bool:
        return self.text.strip() == ""


def parser[Id](
    kind: str | None, scripts_path: Path | None = None
) -> Callable[[Id, Path], Answer[Id]]:
    """
    Return a parser function.
    """

    if kind is None or kind == "text":

        def from_file(id: Id, path: Path) -> Answer[Id]:
            return Answer.from_file(id, path)

        return from_file
    elif ":" in kind:
        if scripts_path is None:
            msg = f"scripts_path must be provided for user-defined parser {kind!r}."
            raise ValueError(msg)
        else:
            return user_parser(kind, scripts_path)
    raise NotImplementedError


def user_parser[Id](
    entry_point: str, scripts_path: Path
) -> Callable[[Id, Path], Answer[Id]]:
    """
    Get a user-defined parser function.

    The function is imported from <scripts_path>/<module_name>.py and must be callable.
    """
    obj = load_entry_point(entry_point, scripts_path, check_callable=True)

    def parser(id: Id, file: Path) -> Answer[Id]:
        out = obj(file)
        return Answer.from_user_data(id, out)

    return parser
