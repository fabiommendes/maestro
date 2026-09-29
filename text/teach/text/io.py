import re
from collections import ChainMap
from dataclasses import dataclass
from typing import Any, Callable, TypeVar, Protocol
from copy import copy

from teach.core import Feedback
from teach.run.utils import extract_numbers, number
from teach.run import BoundRunner

from .pattern import (
    PatternQuestion,
    TextPatternQuestion,
    NumericPatternQuestion,
    WordsPatternQuestion,
    CLASSES as PATTERN_CLASSES,
    CONSTRUCTORS as PATTERN_CONSTRUCTORS,
)

Self = TypeVar("Self", bound="IoQuestion")
Pat = TypeVar("Pat", bound=PatternQuestion)
In = Any

PATTERN_IO_MAP: dict[type[PatternQuestion, "IoQuestion"]] = {}


class IoQuestion(Protocol[Pat]):
    """
    Check inputs against runner and verify if the result follows the expected pattern.
    """

    inputs: In
    pattern: Pat

    @classmethod
    def create_question(cls, inputs, *args, **kwargs) -> "IoQuestion[Any]":
        """
        Simple constructor façade for IoQuestion.
        """
        pat = PatternQuestion.create_question(*args, **kwargs)

        if isinstance(pat, TextPatternQuestion):
            return TextIoQuestion(inputs, pat)
        elif isinstance(pat, NumericPatternQuestion):
            return NumericIoQuestion(inputs, pat)
        elif isinstance(pat, WordsPatternQuestion):
            return WordsIoQuestion(inputs, pat)
        raise TypeError("invalide pattern: %s" % pat)

    @classmethod
    def from_runner(cls, runner: BoundRunner, inputs: In, **kwargs) -> "IoQuestion":
        """
        Create a new Reference instance from text in a file in the filesystem.
        """
        kind = kwargs.get("kind")
        pattern_cls: type[PatternQuestion]
        io_cls: type[IoQuestion]

        if kind is None and cls is IoQuestion:
            msg = 'cannot instantiate abstract class. You must specify the "kind" argument.'
            raise TypeError(msg)
        elif kind is None:
            io_cls = cls
            pattern_cls = cls.__args__[0]
        elif kind not in PATTERN_CLASSES:
            raise ValueError(f"invalid kind: {kind}")
        else:
            pattern_cls = PATTERN_CLASSES[kind]
            io_cls = PATTERN_IO_MAP[pattern_cls]

        data = io_cls.render_input(inputs)
        execution = runner.run_io(data)
        if execution.is_error:
            raise ValueError(f"execution failed: {execution.feedback()}")
        pattern_question = io_cls.parse_output(execution.output, **kwargs)
        return io_cls._build(inputs, pattern_question)

    @staticmethod
    def parse_output(output: str, **kwargs) -> Pat:
        ...

    @classmethod
    def render_input(cls, inpt: In) -> str:
        if isinstance(inpt, (list, tuple)):
            data = "\n".join(map(str, inpt))
        else:
            data = str(inpt)
        return data if data.endswith("\n") else data + "\n"

    @classmethod
    def _build(cls: type[Self], inputs: In, pattern: Pat) -> Self:
        """
        Build a new instance of the question.
        """
        # Be optimistic about the constructor signature
        return cls(inputs=inputs, pattern=pattern)  # type: ignore

    def input_string(self) -> str:
        """
        Return a string representation of the input or None if input data
        was not registered to question.
        """
        return self.render_input(self.inputs)

    def check_text(self, text: str) -> Feedback:
        """
        Check the given output text against the expected content.
        """
        return self.pattern.check_text(text)

    def check_with_runner(
        self, runner: BoundRunner, normalize: Callable[[str], str] | None = None
    ) -> Feedback:
        """
        Pass input to the given runner and verify the results.
        """
        execution = runner.run_io(self.input_string())
        if execution.is_success:
            data = execution.output
            if normalize is not None:
                data = normalize(data)
            return self.check_text(data)
        return execution.feedback()

    def check_function(self, func: Callable[..., str]) -> Feedback:
        """
        Pass inputs to the given runner and verify the results.
        """
        try:
            if isinstance(self.inputs, (list, tuple)):
                args = self.inputs
            else:
                args = (self.inputs,)
            out = func(*args)
        except Exception as ex:
            return Feedback.Fail("exception", str(ex), payload=ex)

        if isinstance(out, str):
            return self.check_text(out)

        msg = f"function did not return string, got {type(out)}"
        return Feedback.Fail("error", msg, payload=type(out))

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
class TextIoQuestion(IoQuestion[TextPatternQuestion]):
    """
    Checks if the output is equal to the expected output.
    """

    inputs: In
    pattern: TextPatternQuestion

    @classmethod
    def parse_output(cls, output: str, kind=None, **kwargs) -> TextPatternQuestion:
        if kind and PATTERN_CLASSES[kind] is not TextPatternQuestion:
            raise ValueError(f"invalid kind: {kind}")
        elif kind:
            return PATTERN_CONSTRUCTORS[kind](output, **kwargs)
        return TextPatternQuestion(output, **kwargs)


@dataclass(frozen=True)
class NumericIoQuestion(IoQuestion[NumericPatternQuestion]):
    """
    Checks if numbers that appear in output are equal or similar to
    an expected pattern.

    Other textual elements are ignored.
    """

    inputs: In
    pattern: NumericPatternQuestion

    @classmethod
    def parse_output(cls, output: str, kind=None, **kwargs) -> NumericPatternQuestion:
        numbers = extract_numbers(output, to_number=number)
        if kind and PATTERN_CLASSES[kind] is not NumericPatternQuestion:
            raise ValueError(f"invalid kind: {kind}")
        elif kind:
            return PATTERN_CONSTRUCTORS[kind](output, **kwargs)
        return NumericPatternQuestion(numbers, **kwargs)


@dataclass(frozen=True)
class WordsIoQuestion(IoQuestion[WordsPatternQuestion]):
    """
    Question that checks if strings or patterns appearn in order.

    Other textual elements are ignored.
    """

    inputs: In
    pattern: WordsPatternQuestion

    @classmethod
    def parse_output(cls, output: str, kind=None, **kwargs) -> WordsPatternQuestion:
        regex = re.compile(r"[^\s,.!?;()[\]]+")
        words = (m.group(0) for m in regex.finditer(output))

        if kind and PATTERN_CLASSES[kind] is not WordsPatternQuestion:
            raise ValueError(f"invalid kind: {kind}")
        elif kind:
            return PATTERN_CONSTRUCTORS[kind](output, **kwargs)

        if not kwargs.get("ordered", False):
            words = set(words)  # type: ignore
        return WordsPatternQuestion([*words], **kwargs)


PATTERN_IO_MAP.update(
    {
        TextPatternQuestion: TextIoQuestion,
        NumericPatternQuestion: NumericIoQuestion,
        WordsPatternQuestion: WordsIoQuestion,
    }
)
