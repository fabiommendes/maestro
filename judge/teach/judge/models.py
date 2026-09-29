from collections import ChainMap
from dataclasses import dataclass, field
from multiprocessing.sharedctypes import Value
from typing import Any, Callable, TypeVar
from functools import cached_property
from pathlib import Path
from copy import copy

from teach.core import Feedback
from teach.run import (
    CodeTransform,
    BoundRunnerDelegate,
    BoundRunner,
    Lang,
    source_runner,
)
from teach.text import IoQuestion

from .util import code_transform

LineTransform = Callable[[str], str]
IoTests = list[IoQuestion]
Self = TypeVar("Self", bound="Base")


class Base(BoundRunnerDelegate):
    """
    Shared implementation between Question and Submission.
    """

    # Expected instance variables
    src: str
    from_source: Callable
    timeout: float | None

    # Class variables and properties
    _lang_attr = "lang"
    _lang = property(lambda self: getattr(self, self._lang_attr))

    @property
    def _runner_instance(self) -> "BoundRunner":
        return source_runner(self.src, self._lang, timeout=self.timeout)

    @classmethod
    def from_path(cls: type[Self], path: Path, *args, **kwargs) -> Self:
        """
        Create a new Reference instance from a source code string.
        """
        code = code_transform(path)
        kwargs.setdefault(cls._lang_attr, code.lang)
        return cls.from_source(path.read_text(), *args, **kwargs)

    def run_test(self, input: str) -> Any:
        """
        Run code with the given input and return the results.
        """
        return self._runner_instance.run_io(input)

    def copy(self: Self, **kwargs) -> Self:
        """
        Return a copy of question.
        """
        cls = type(self)
        opts = {k: copy(v) for k, v in kwargs.items()}
        return cls(**ChainMap(opts, self.__dict__))  # type: ignore

    def to_dict(self) -> dict[str, Any]:
        """
        Return a dictionary representation of the question.
        """
        return {k: copy(v) for k, v in self.__dict__.items()}


@dataclass(frozen=True)
class Question(Base):
    """
    Defines a programming program that is evaluated by an online judge.
    """

    src: str
    ref_lang: Lang
    header: str = ""
    timeout: float = 1.0
    tests: IoTests = field(default_factory=list, repr=False)
    accepted_langs: set[Lang] = field(default_factory=set)

    _lang_attr = "ref_lang"

    @cached_property
    def ref_code(self) -> CodeTransform[str]:
        return code_transform(self.ref_lang)

    @classmethod
    def from_source(
        cls, src: str, ref_lang: Lang | CodeTransform[str], **kwargs
    ) -> "Question":
        """
        Create a new Reference instance from a source code string.
        """
        code: CodeTransform[str]
        code = code_transform(ref_lang)
        header, src = code.split_comment_header(src)
        new = cls(src, code.lang, header=header, **kwargs)
        object.__setattr__(new, "ref_code", code)  # type: ignore
        return new

    def __post_init__(self):
        if not self.accepted_langs:
            self.accepted_langs.add(self.ref_lang)

    def new_submission(self, src: str, **kwargs) -> "Submission":
        """
        Create a new Submission instance from a source code string.
        """
        if isinstance(src, str):
            return Submission.from_source(src, self.ref_lang, self, **kwargs)
        elif isinstance(src, Path):
            return Submission.from_path(src, self, **kwargs)
        else:
            raise TypeError(f"invalid source: {type(src)}")

    def check_submission(
        self, submission: "Submission", timeout: float | None = None
    ) -> Feedback:
        """
        Check if the submission is valid.
        """

        if submission.lang not in self.accepted_langs:
            return Feedback.Fail(
                "invalid",
                f"Submission language is not accepted: {submission.lang}.",
            )

        feedback = submission.code.check_syntax(submission.src)
        if feedback.is_error:
            return feedback

        runner = self._runner_instance
        for test in self.tests:
            feedback = test.check_with_runner(runner)
            if feedback.is_error:
                return feedback

        return Feedback.Success()

    def add_test(self, test: IoQuestion, verify=True):
        """
        Add new test to the list of tests if test is accepted
        with the given program.
        """
        if verify:
            feedback = test.check_with_runner(self._runner_instance)
            if feedback.is_error:
                raise ValueError("test faild in reference implementation", feedback)
        return self.tests.append(test)

    def add_expected(self, inputs: Any, *args, verify=True, **kwargs):
        """
        Run example and add it to the list of test examples.
        """
        test = IoQuestion.create_question(inputs, *args, **kwargs)
        self.add_test(test, verify=verify)

    def add_example(self, inputs: Any, *, kind="text", **kwargs):
        """
        Run example and add it to the list of test examples.
        """
        kwargs["kind"] = kind
        test = IoQuestion.from_runner(self._runner_instance, inputs, **kwargs)
        self.add_test(test, verify=False)


@dataclass(frozen=True)
class Submission(Base):
    """
    A submission to an automatic judge problem.
    """

    question: Question
    src: str
    lang: Lang
    header: str = ""

    @cached_property
    def code(self) -> CodeTransform[str]:
        return code_transform(self.lang)

    @classmethod
    def from_source(
        cls, src: str, lang: Lang, question: "Question", **kwargs
    ) -> "Submission":
        """
        Read Submission instance from a source code string.
        """
        code = code_transform(lang)
        header, src = code.split_comment_header(src)
        return cls(question, src, code.lang, header, **kwargs)
