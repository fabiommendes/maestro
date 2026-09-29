from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from itertools import zip_longest
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    Iterator,
    Tuple,
    Dict,
    TypeVar,
    overload,
)
from functools import cached_property
from operator import attrgetter
from pathlib import Path
import random

from teach.core import Feedback
from teach.core.string import split_indentation
from teach.run import CodeTransform, Lang, Text, source_runner
from teach.text import IoQuestion


from .util import code_transform

F = TypeVar("F", Lang, Text)
LineTransform = Callable[[str], str]


@dataclass(frozen=True)
class Question(Generic[F]):
    """
    Defines a line permutation problem and store a reference solution.
    """

    lang: F
    lines: list["Line"]
    header: str = ""
    allow_duplicates: bool = False
    tests: list[IoQuestion] = field(default_factory=list)

    @cached_property
    def code(self) -> CodeTransform[str]:
        return code_transform(self.lang)

    @classmethod
    def from_source(
        cls, src: str, lang: Lang | CodeTransform[str], **kwargs
    ) -> "Question":
        """
        Create a new Reference instance from a source code string.
        """
        code: CodeTransform[str]
        code = code_transform(lang)
        header, src = code.split_comment_header(src)
        lines = [*parse_lines(src, code)]
        return cls(code.lang, lines, header, **kwargs)

    @classmethod
    def from_path(cls, path: Path, **kwargs) -> "Question":
        """
        Create a new Reference instance from a source code string.
        """
        code = code_transform(path)
        question = cls.from_source(path.read_text(), lang=code, **kwargs)

        # Add IO tests data
        inputs = outputs = None
        if (examples_in := path.with_suffix(".input")).exists():
            inputs = examples_in.read_text().split("\n---")
        if (examples_out := path.with_suffix(".output")).exists():
            outputs = examples_out.read_text().split("\n---")
        if inputs is None or outputs is None:
            return question

        for examples_in, out_ in zip_longest(inputs, outputs):
            question.tests.append(IoQuestion.create_question(examples_in, equal=out_))
        return question

    def new_submission(self, src: str, **kwargs) -> "Submission":
        """
        Create a new Submission instance from a source code string.
        """
        return Submission.from_source(src, self, **kwargs)

    def render(self, skip_comments: bool = False) -> str:
        """
        Render the reference source code as a string.
        """
        body = render_lines(self.lines, self.code, skip_comments)
        if self.header:
            head = self.code.render_block_comment(self.header)
            return f"{head}\n\n{body}"
        return body

    def copy(self, *, lines=None, tests=None, **kwargs) -> "Question":
        """
        Return a copy of question.
        """
        cls = type(self)
        kwargs.setdefault("lang", self.lang)
        kwargs.setdefault("allow_duplicates", self.allow_duplicates)
        kwargs.setdefault("header", self.header)

        # Mutable types
        if lines is None:
            lines = [line.copy() for line in self.lines]
        else:
            kwargs["lines"] = self.lines.copy()

        if tests is None:
            kwargs["tests"] = self.tests.copy()
        else:
            kwargs["tests"] = list(lines)

        return cls(**kwargs)

    def to_dict(self) -> Dict[str, Any]:
        """
        Return a dictionary representation of the question.
        """
        return {
            "lang": self.lang,
            "lines": [line.to_dict() for line in self.lines],
            "allow_duplicates": self.allow_duplicates,
            "description": self.header,
            "tests": self.tests,
        }

    def check_submission(
        self,
        submission: "Submission",
        timeout: float | None = None,
        line_transform: LineTransform | None = None,
        transform_output: bool = False,
    ) -> Feedback:
        """
        Check if the submission is valid.
        """

        if line_transform is None:
            sub = submission
        else:
            lines = lines = map_lines(line_transform, self.lines)
            qst = self.copy(lines=lines)

            lines = map_lines(line_transform, submission.lines)
            sub = submission.copy(question=qst, lines=lines)

        if sub.is_reference_answer():
            return Feedback.Success()

        extra_lines = sub.extra_lines()
        if extra_lines:
            return Feedback.Fail(
                "extra-lines",
                "You are not allowed to create or modify existing lines",
                extra_lines,
            )

        src = submission.render(skip_comments=True)
        result = self.code.check_syntax(src)
        if result.is_error:
            return result

        if self.tests:
            runner = source_runner(src, self.code, timeout=timeout)
            for test in self.tests:
                fn = line_transform if transform_output else None
                result = test.check_with_runner(runner, fn)
                if result.is_error:
                    return result
        else:
            return Feedback.Fail("no-tests", "Not the reference solution")

        return Feedback.Success()


@dataclass(frozen=True)
class Submission(Sequence):
    """
    A submission to a line permutation problem.
    """

    question: Question = field(repr=False, compare=False)
    lines: list["Line"]
    header: str = ""

    @property
    def code(self) -> CodeTransform[str]:
        return self.question.code

    @classmethod
    def from_source(cls, src: str, question: "Question") -> "Submission":
        """
        Read Submission instance from a source code string.
        """
        code = question.code
        header, src = code.split_comment_header(src)
        return cls(question, list(parse_lines(src, code)), header)

    def __len__(self) -> int:
        return len(self.lines)

    @overload
    def __getitem__(self, index: int) -> "Line":
        ...

    @overload
    def __getitem__(self, index: slice) -> list["Line"]:
        ...

    def __getitem__(self, index):  # type: ignore
        return self.lines[index]

    def __iter__(self) -> Iterator["Line"]:
        return iter(self.lines)

    def render(self, skip_comments: bool = False) -> str:
        """
        Render the submission permutations as a string of code.
        """
        return render_lines(self.lines, self.code, skip_comments)

    def is_full_permutation(self) -> bool:
        """
        Check if the submission uses all lines in reference exactly once.
        """
        subs = Counter(line.src for line in self)
        refs = Counter(line.src for line in self.question.lines)
        return subs == refs

    def is_permutation(self, comments: bool = False) -> bool:
        """
        Check if all submission lines are present in reference solution.

        If comments=True, verify that comments are also present in solution.
        """
        pred = lambda _: True if comments else None
        lines = map(_get_source, filter(pred, self.lines))
        duplicates = self.question.allow_duplicates
        ref_counts = Counter(map(_get_source, self.question.lines))

        for k, v in Counter(lines).items():
            if duplicates and (n := ref_counts[k]) > 0:
                v = min(v, n)
            ref_counts[k] -= v

        return all(x >= 0 for x in ref_counts.values())

    def is_reference_answer(self) -> bool:
        """
        Check if the submission is identical to the reference answer to the problem.
        """
        submission = [*filter(None, self.lines)]
        solution = [*filter(None, self.question.lines)]
        return all(x == y for x, y in zip_longest(submission, solution))

    def extra_lines(self, comments: bool = False) -> list[Tuple[int, str]]:
        """
        Return a list of lines that are not in the reference solution.

        If comments=True, verify that comments are also present in solution.
        """
        extra = []
        refs = Counter(map(_get_source, self.question.lines))
        for i, line in enumerate(self.lines):
            src = line.src
            if comments or line.enabled:
                refs[src] -= 1
            if refs[src] < 0:
                extra.append((i, src))
        return extra

    def copy(self, *, lines=None, **kwargs) -> "Submission":
        """
        Return a copy of the submission.
        """
        cls = type(self)
        kwargs.setdefault("header", self.header)
        kwargs.setdefault("question", self.question)

        if lines is None:
            kwargs["lines"] = [line.copy() for line in self.lines]
        else:
            kwargs["lines"] = self.lines.copy()

        return cls(**kwargs)

    def to_dict(self, only_lines=False) -> Dict[str, Any]:
        """
        Return a JSON-compatible dictionary representation of submission.
        """
        lines = [line.to_dict() for line in self.lines]
        if only_lines:
            return {"lines": lines}
        return {"question": self.question.to_dict(), "lines": lines}


@dataclass(frozen=True)
class Line:
    """
    A line permutation.
    """

    src: str
    indentation: str
    enabled: bool

    @property
    def is_comment(self) -> bool:
        return not self.enabled

    @classmethod
    def from_source(cls, line: str, code: CodeTransform) -> "Line":
        """
        Create a new LinePermRef from a source line.
        """
        indentation, line = split_indentation(line)
        if code.is_line_comment(line):
            enabled = False
            line = code.extract_line_comment_data(line)
        else:
            enabled = True
        return cls(line, indentation, enabled)

    def __bool__(self) -> bool:
        return self.enabled

    def render(self, code: CodeTransform) -> str:
        """
        Renders line.
        """
        if self.src.isspace():
            return ""

        if self.enabled:
            line = self.src
        else:
            line = code.render_line_comment(self.src)
        return self.indentation + line

    def copy(self, **kwargs) -> "Line":
        """
        Return a copy of line.
        """
        cls = type(self)
        if kwargs:

            kwargs.setdefault("src", self.src)
            kwargs.setdefault("indentation", self.indentation)
            kwargs.setdefault("enabled", self.enabled)
            return cls(**kwargs)
        return cls(self.src, self.indentation, self.enabled)

    def to_dict(self) -> Dict[str, Any]:
        """
        Return a JSON-compatible dictionary representation of the line.
        """
        return {
            "src": self.src,
            "indentation": self.indentation,
            "enabled": self.enabled,
        }


_get_source = attrgetter("src")


def randomize_lines(
    lines: Iterable[Line], seed: bytes | int | None = None
) -> list[Line]:
    """
    Shuffle the lines in the reference solution and return a new reference.
    """

    rand = random.Random(seed)
    lines = list(lines)
    indentations = [line.indentation for line in lines]

    rand.shuffle(lines)
    rand.shuffle(indentations)

    for i, line in enumerate(lines):
        lines[i] = line.copy(indentation=indentations[i], enabled=rand.random() < 0.75)
    return lines


def simplify_lines(
    lines: Iterable[Line],
    indentation: bool = True,
    strip_comments: bool = False,
    alphabetic: bool = False,
) -> list[Line]:
    """
    Remove all indentation from the reference solution and return a new reference.
    """
    kwargs: dict[str, Any] = {}
    if indentation:
        kwargs["indentation"] = ""
    if strip_comments:
        kwargs["enabled"] = True

    lines = (line.copy(**kwargs) for line in lines)
    if alphabetic:
        line_lst = sorted(lines, key=lambda line: (not line.enabled, line.src))
    else:
        line_lst = list(lines)
    return line_lst


def map_lines(fn: Callable[[str], str], lines: Iterable[Line]) -> list[Line]:
    """
    Apply a function to all lines in the reference solution and return a new reference.
    """
    return [line.copy(src=fn(line.src)) for line in lines]


def parse_lines(src: str, code: CodeTransform[str]) -> Iterator["Line"]:
    """
    Parse lines from a source code string.

    Yield lines as a generator.
    """
    for line in src.splitlines():
        if line.strip():
            yield Line.from_source(line, code)


def render_lines(
    lines: Iterable[Line], code: CodeTransform, skip_comments: bool = False
) -> str:
    """
    Render lines as a source code string.
    """
    if skip_comments:
        lines = filter(None, lines)
    return "\n".join(line.render(code) for line in lines)
