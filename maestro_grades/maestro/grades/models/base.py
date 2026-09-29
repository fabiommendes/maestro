from collections import UserDict
from enum import IntEnum
from fractions import Fraction
from decimal import Decimal
from typing import Iterable, Mapping, Optional, TypeVar, TYPE_CHECKING, Union

from teach.core import Model, CompetencyId, Grade, Grades

if TYPE_CHECKING:
    from rich.console import Console

    from .question import Question
    from .exam import Exam
    from .student import Student
    from .exercise import Competency

    Model = Model
    Grade = Grade
    Grades = Grades


# Type aliases
QuestionId = str
QuestionRef = Union[QuestionId, "Question"]
StudentId = str
StudentRef = Union[StudentId, "Student"]
CompetencyRef = Union[CompetencyId, "Competency"]
ExamId = str
ExamRef = Union[ExamId, "Exam"]
ConsoleT = Optional["Console"]
GradesDb = dict[StudentId, Grades]

# Auxiliary types
T = TypeVar("T")


class DataMapping(Mapping[str, T]):
    """
    Base class for Exam and Exercise.

    Implements a mapping that proxies its .data attribute.
    """

    data: dict[str, T]

    # Start by borrowing-out the abstract methods from UserDict
    __init__ = UserDict.__init__  # type: ignore
    __len__ = UserDict.__len__  # type: ignore
    __getitem__ = UserDict.__getitem__  # type: ignore
    __setitem__ = UserDict.__setitem__  # type: ignore
    __delitem__ = UserDict.__delitem__  # type: ignore
    __iter__ = UserDict.__iter__  # type: ignore
    __contains__ = UserDict.__contains__  # type: ignore

    # We coerce results to dictionaries to avoid ambiguity on
    # what to do with the extra non-mapping information.
    def __or__(self, other):
        return self.data.__or__(other)

    def __ror__(self, other):
        return self.data.__ror__(other)

    def __ior__(self, other):
        if isinstance(other, DataMapping):
            other = other.data
        self.data |= other
        return self


class Level(IntEnum):
    """
    Standardized competency level
    """

    #: Little or no exposure to competency.
    NO_EXPERIENCE = 0

    #: General awareness of concepts and competency.
    TRAINING = 1

    #: Can apply standard methods in solving standardized problems
    UNDERSTANDING = 2

    #: Can apply methods in solving problems of moderate complexity
    #: that require some original thinking.
    CREATIVE = 3

    #: Is independent and can formulate novel problems and approaches on
    #: their own.
    KNOWLEDGE = 4

    #: Can prove independency in formulating problems
    MATURE = 5

    @classmethod
    def from_progress(cls, xs: Iterable["Progress"]):
        """
        Return the competency level from the given sequence of progress values.

        It will return the highest competency that has a total score of at least 1.
        """
        from collections import Counter

        data = Counter((x.level, x) for x in xs)
        levels = (k for k, v in data.items() if v >= 1)
        try:
            return max(levels)
        except IndexError:
            return cls.NO_EXPERIENCE


class Progress(Fraction):
    """
    Competency progress towards some level.
    """

    def __init__(self, *args, level: Level):
        super().__init__(*args)
        self.level = level


def get_console(console: ConsoleT = None) -> "Console":
    """
    Return the console to use for printing.
    """
    if console is None:
        from rich import get_console as _get_console

        return _get_console()
    return console


# Clean namespace
del Fraction, Decimal, TypeVar, Mapping, Iterable, UserDict, IntEnum
