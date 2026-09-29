from collections import ChainMap, deque
from dataclasses import dataclass, field
from email import feedparser
import re
from typing import Any, Callable, Generic, Iterator, Pattern, TypeVar, Protocol
from pathlib import Path
from copy import copy

from teach.core import Feedback
from teach.core.string import clean_string
from teach.run.utils import extract_numbers, number
from teach.run import (
    BoundRunner,
)
from unidecode import unidecode

# TODO: Think of a better architecture for this.
# IoCheck should be divided in two parts: one simply checks the output, and
# can be composed at will.
#
# The second part specify how the input should be constructed.
#
Self = TypeVar("Self", bound="PatternQuestion")
T = TypeVar("T")
N = int | float
RePat = str | Pattern
Pat = TypeVar("Pat")
StrFn = Callable[[str], str]

CONSTRUCTORS: dict[str, Callable[[Any], "PatternQuestion[Any]"]] = {
    "exact": lambda pat, **kw: TextPatternQuestion(pat, **kw),
    "iexact": lambda pat, **kw: TextPatternQuestion(pat, casefold=True, **kw),
    "equal": lambda pat, **kw: TextPatternQuestion(pat, **_text_eq, **kw),  # type: ignore
    "iequal": lambda pat, **kw: TextPatternQuestion(pat, casefold=True, **_text_eq, **kw),  # type: ignore
    "regex": lambda pat, **kw: TextPatternQuestion(_re_compile(pat), **kw),  # type: ignore
    "words": lambda seq, **kw: WordsPatternQuestion(seq, ordered=True, **kw),
    "contains": lambda seq, **kw: WordsPatternQuestion(seq, ordered=False, **kw),
    "numbers": lambda nums, **kw: NumericPatternQuestion(nums, **kw),
    "floats": lambda nums, **kw: NumericPatternQuestion(nums, tol=1e-6, **kw),
}
CLASSES: dict[str, type["PatternQuestion[Any]"]] = {}

_re_compile = lambda x: re.compile(x) if isinstance(x, str) else x
_text_eq = {"clean_spaces": True, "unidecode": True}


@dataclass(frozen=True)
class PatternQuestion(Generic[Pat]):
    """
    Question evaluates if string matches some pattern.
    """

    pattern: Pat
    casefold: bool = field(default=False, kw_only=True)
    unidecode: bool = field(default=False, kw_only=True)
    clean_spaces: bool = field(default=False, kw_only=True)
    normalizer: StrFn | None = field(default=None, kw_only=True, repr=False)

    @classmethod
    def create_question(cls, *args, **kwargs) -> "PatternQuestion[Any]":
        """
        Simple constructor for PatternQuestion.
        """
        if len(args) == 2 and not kwargs:
            name, value = args
            return CONSTRUCTORS[name](value)

        elif not args:
            is_any = kwargs.pop("any", False)
            parts = [cls.create_question(*item) for item in kwargs.items()]
            match len(parts), is_any:
                case 0, _:
                    pass
                case 1, _:
                    return parts[0]
                case _, False:
                    return AllPatternQuestion(parts)
                case _, True:
                    return AnyPatternQuestion(parts)

        msg = "expect two positional arguments or one or more keyword arguments"
        raise TypeError(msg)

    def check_text(self, text: str) -> Feedback:
        """
        Check the given output text against the expected content.
        """
        ...

    def check_io(self, runner: BoundRunner, input: str) -> Feedback:
        """
        Pass input to the given runner and verify the results.
        """
        execution = runner.run_io(input)
        if execution.is_success:
            return self.check_text(execution.output)
        return execution.feedback()

    def check_function(self, func: Callable[[T], str], *args: T) -> Feedback:
        """
        Pass input to the given runner and verify the results.
        """
        try:
            out = func(*args)
        except Exception as ex:
            return Feedback.Fail("exception", str(ex), payload=ex)

        if isinstance(out, str):
            return self.check_text(out)

        msg = f"function did not return string, got {type(out)}"
        return Feedback.Fail("error", msg, payload=type(out))

    def clean(self, text: str) -> str:
        """
        Normalize the text for comparison.
        """
        if self.unidecode:
            text = unidecode(text)
        if self.casefold:
            text = text.casefold()
        if self.normalizer is not None:
            text = self.normalizer(text)
        if self.clean_spaces:
            text = "\n".join(map(str.rstrip, text.splitlines()))
        return text

    def copy(self: Self, **kwargs) -> Self:
        """
        Return a copy of question.
        """
        cls = type(self)
        opts = {k: copy(v) for k, v in kwargs.items()}
        return cls(**ChainMap(opts, self.__dict__))  # type: ignore

    def to_dict(self) -> dict[str, Any]:
        """
        Return a dictionary representation.
        """
        return {k: copy(v) for k, v in self.__dict__.items()}


@dataclass(frozen=True)
class BoolPatternQuestion(PatternQuestion[bool]):
    """
    A question that conditionally accepts or rejects the output.
    """

    @property
    def is_success(self) -> bool:
        return self.pattern

    @property
    def is_failure(self) -> bool:
        return not self.pattern

    @classmethod
    def always_fail(cls) -> "BoolPatternQuestion":
        return cls(False)

    @classmethod
    def always_succeed(cls) -> "BoolPatternQuestion":
        return cls(True)

    def check_text(self, text: str) -> Feedback:
        if self.pattern:
            return Feedback.Success()
        else:
            msg = "Result was unconditionally rejected"
            return Feedback.Fail("error", msg, payload=text)


@dataclass(frozen=True)
class TextPatternQuestion(PatternQuestion[RePat]):
    """
    Checks that the output is equal to an expected string or regular
    expression pattern.

    If pattern is a regular expression, it checks the output against
    the regex using re.fullmatch semantics.
    """

    def check_text(self, text: str) -> Feedback:
        try:
            check = self.pattern.fullmatch  # type: ignore
        except AttributeError:
            check = lambda x: self.clean(self.pattern) == x  # type: ignore

        if check(self.clean(text)):
            return Feedback.Success()
        else:
            payload = (self.pattern, text)
            return Feedback.Fail("error", "Different outputs", payload=payload)


@dataclass(frozen=True)
class NumericPatternQuestion(PatternQuestion[list[N]]):
    """
    Question that checks if numbers in input and output are equal or similar.

    Other textual elements are ignored.
    """

    tol: float = field(default=0, kw_only=True)
    comparator: Callable[[N, N, float], bool] | None = field(default=None, kw_only=True)

    def compare_numbers(self, x: N, y: N):
        if self.comparator is not None:
            return self.comparator(x, y, self.tol)
        if x == y:
            return True
        if x * y < 0:
            return False
        if abs(x - y) / abs(x + y) <= self.tol:
            return True
        if abs(x - y) <= self.tol:
            return True
        return False

    def check_text(self, text: str) -> Feedback:
        cmp = self.compare_numbers
        errors = []
        matches = deque(extract_numbers(text))
        values = deque(self.pattern)

        while matches and values:
            x = values.popleft()
            m = matches.popleft()
            if m == str(x):
                continue
            try:
                n = number(m)
            except ValueError:
                errors.append(f"invalid number: {m}")
                continue
            else:
                if not cmp(x, n):
                    errors.append(f"got {m}, expect {x}")

        if matches:
            extra = ", ".join(m for m in matches)
            errors.append(f"output has extra numbers: {extra}")
        if values:
            extra = ", ".join(map(str, values))
            errors.append(f"missing numbers: {extra}")

        if errors:
            return Feedback.Multi([Feedback.Fail("error", msg) for msg in errors])
        return Feedback.Success()


@dataclass(frozen=True)
class WordsPatternQuestion(PatternQuestion[list[RePat]]):
    """
    Checks if strings or patterns appear in text.

    If ordered = True, verify if exemples are present in text, in order.
    """

    ordered: bool = field(default=False, kw_only=True)

    def check_text(self, text: str) -> Feedback:
        if self.ordered:
            errors = list(self._check_ordered(self.clean(text)))
        else:
            errors = list(self._check_non_ordered(self.clean(text)))
        return Feedback.Multi(errors)

    def _check_ordered(self, text: str) -> Iterator[Feedback]:
        # TODO: implement an algorithm that detects strings out of order.
        for pat in self.pattern:
            if isinstance(pat, str):
                _, sep, tail = text.partition(pat)
                if not sep:
                    yield Feedback.Fail("error", f"missing string: {pat}")
                else:
                    text = tail

            elif isinstance(pat, Pattern):
                *pre, tail = pat.split(text, 1)
                if not pre:
                    yield Feedback.Fail("error", f"missing pattern: {pat}")
                else:
                    text = tail

    def _check_non_ordered(self, text: str) -> Iterator[Feedback]:
        for pat in self.pattern:
            if isinstance(pat, str) and pat not in text:
                yield Feedback.Fail("error", f"missing string: {pat}")

            elif isinstance(pat, Pattern) and pat.search(text):
                yield Feedback.Fail("error", f"missing pattern: {pat}")


@dataclass(frozen=True)
class AnyPatternQuestion(PatternQuestion[list[PatternQuestion[Any]]]):
    """
    Checks if any of the given patterns match the text.
    """

    def check_text(self, text: str) -> Feedback:
        errors = []
        for pat in self.pattern:
            feedback = pat.check_text(text)
            if feedback.is_success:
                return Feedback.Success()
            errors.append(feedback)
        return Feedback.Multi(errors)


@dataclass(frozen=True)
class AllPatternQuestion(PatternQuestion[list[PatternQuestion[Any]]]):
    """
    Checks if all of the given patterns match the text.
    """

    def check_text(self, text: str) -> Feedback:
        return Feedback.Multi([pat.check_text(text) for pat in self.pattern])


CLASSES.update(
    {
        "exact": TextPatternQuestion,
        "iexact": TextPatternQuestion,
        "equal": TextPatternQuestion,
        "iequal": TextPatternQuestion,
        "regex": TextPatternQuestion,
        "words": WordsPatternQuestion,
        "contains": WordsPatternQuestion,
        "numbers": NumericPatternQuestion,
        "floats": NumericPatternQuestion,
    }
)
