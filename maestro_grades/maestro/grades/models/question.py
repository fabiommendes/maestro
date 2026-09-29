import abc
from dataclasses import dataclass, field
from enum import Enum
import json
import tempfile
from pathlib import Path
from typing import Any, ClassVar, Generic, TypeVar, Union, TYPE_CHECKING

from rich import get_console
from rich.syntax import Syntax
from rich.prompt import Prompt
from sidekick.properties import lazy
import unidecode

from teach.core import Model, ParentMixin, Feedback, Assessment
from teach import lineperm
from teach import judge

from .submission import Submission, TextSubmission, PathSubmission
from .base import Grades, Model, QuestionId
from .. import config
from ..utils import number

if TYPE_CHECKING:
    from .exam import Exam

__all__ = [
    "Question",
    "Question",
    "EmptyQuestion",
    "ManualQuestion",
    "LinePermQuestion",
]

# Declare types
IoInputAtom = Union[int, float, str]
IoInput = Union[IoInputAtom, list[IoInputAtom], tuple[IoInputAtom, ...]]
Sub = TypeVar("Sub", bound=Submission)


class Method(Enum):
    """
    Enum describing available grading methods.
    """

    OnlyAuto = "only-auto"
    TryAuto = "try-auto"
    Manual = "manual"
    Skip = "skip"


class Question(abc.ABC, Model, ParentMixin, Generic[Sub]):
    """
    Abstract class for atomic assessment task.

    Attributes:
        id:
            Question ID.
        title:
            A human-friendly question title.
        description:
            A human-friendly and detailed description of question.
        is_active:
            Whether the question is active and should be graded.
        memo:
            Memoization dictionary for grading. It stores submissions
            and their respective grades. This may accelerate both automatic
            and manual grading tasks by re-using previously computed
            assesments.
    """

    # Constructor attributes
    id: QuestionId
    title: str = ""
    description: str = ""
    is_active: bool = True

    # Inner attributes
    memo: dict[bytes, Grades] | None = None
    competencies: Grades
    competencies = lazy(lambda _: dict)  # type: ignore

    # Class variables
    submission_class: ClassVar[type[Submission]]
    OnlyAuto: ClassVar[Method] = Method.OnlyAuto
    TryAuto: ClassVar[Method] = Method.TryAuto
    Manual: ClassVar[Method] = Method.Manual
    Skip: ClassVar[Method] = Method.Skip
    default_grading_method: ClassVar[Method] = Method.TryAuto

    # Derived properties
    @property
    def exam(self) -> "Exam":
        exam = self.parent
        if exam is None:
            raise ValueError("Question is not part of an exam.")
        return exam

    @exam.setter
    def exam(self, exam: "Exam") -> None:
        self.parent = exam

    @property
    def data_path(self):
        "Base path to a directory containing question data."
        return self.exam.questions_path.joinpath(self.id).resolve()

    def prepare(self):
        """
        Prepare question for a grading task.

        Default implementation is a no-op. But this step may be used to organize
        assets, fetch data from the internet, compile programs, etc. Assets
        organized in this state are shared across all submissions.
        """

    def memo_key(self, submission: Sub) -> bytes:
        """
        Return a key for memoization.
        """
        return submission.hash()

    def grade_submission(self, submission: Sub, method: Method) -> Assessment:
        """
        Grade student submission and return a Feedback.
        """

        match method:
            case Method.OnlyAuto:
                try:
                    fn = self.grade_submission_auto  # type: ignore
                except AttributeError:
                    raise NotImplementedError("Question has no auto-grading method.")
                return fn(submission)

            case Method.TryAuto:
                try:
                    res = self.grade_submission(submission, Method.OnlyAuto)
                except NotImplementedError:
                    mode = Assessment.MODE_SKIP
                    msg = "No auto-grading method."
                    res = Assessment({}, mode=mode, comment=msg)
                if res.is_skipped:
                    res = self.grade_submission(submission, Method.Manual)
                return res

            case Method.Manual:
                # Consult memo dictioary to see if we have already graded this submission.
                if self.memo is not None:
                    key = self.memo_key(submission)
                    try:
                        grades = self.memo[key]
                    except KeyError:
                        pass
                    else:
                        msg = f"Reused previous assessment, with key {key.hex()}."
                        mode = Assessment.MODE_AUTO
                        return Assessment(grades, mode=mode, comment=msg)

                # Manually grade it
                try:
                    fn = self.grade_submission_manual  # type: ignore
                except AttributeError:
                    res = _manually_grade_submission(self, submission)
                else:
                    res = fn(submission)
                return res

            case Method.Skip:
                return Assessment({}, mode=Assessment.MODE_SKIP)

            case m if isinstance(m, str):
                return self.grade_submission(submission, Method(m))

            case None:
                return self.grade_submission(submission, self.default_grading_method)

            case _:
                raise ValueError(f"Invalid grading method: {method}")

    def display_submission(self, submission: Sub, console=get_console()) -> None:
        """
        Display student submission in a rich text console.
        """
        console.print(submission, highlight=True)


class QuestionWithFile(Question[TextSubmission]):
    """
    Text/bytes based submissions.
    """

    file: Path
    submission_class = TextSubmission

    @property
    def reference_file(self) -> Path:
        "Path to the file containing reference solution."
        return self.data_path.joinpath(self.file)

    @property
    def reference_text(self) -> str:
        "Text in the reference solution."
        return self.reference_file.read_text()


class QuestionWithRepo(Question[PathSubmission]):
    """
    Submissions are an entire repository.
    """


@dataclass
class EmptyQuestion(QuestionWithFile):
    """
    Empty question. Just skip grading.
    """

    id: QuestionId
    title: str = ""
    description: str = "Disabled"
    message: str = "This question was disabled during grading."
    is_active: bool = True
    default_grading_method = Method.Skip

    def grade_submission_auto(self, _) -> Feedback:
        return Feedback.Skip("Question cannot be graded.")


@dataclass
class LinePermQuestion(QuestionWithFile):
    """
    Skip correction.

    It can provide some explanation message.
    """

    id: QuestionId
    question: lineperm.Question
    file: Path
    timeout: float = 10.0
    title: str = ""
    description: str = ""
    casefold: bool = False
    unidecode: bool = False
    remove_comments: bool = True
    remove_trailing_spaces: bool = True

    def grade_submission_auto(self, submission: TextSubmission) -> Assessment:
        options = {
            "line_transform": self._line_transform_func(),
            "timeout": self.timeout,
        }
        lineperm_sub = self.question.new_submission(submission.text)
        fb = self.question.check_submission(lineperm_sub, **options)
        return Assessment.from_feedback(fb, key=self.id)

    def _line_transform_func(self) -> lineperm.LineTransform | None:
        # if not self.casefold and not self.unidecode:
        #     return None

        def transform(line: str) -> str:
            old = line
            if self.remove_comments:
                # FIXME: this is a python specifc hack to remove comments.
                line, _, _ = line.partition("#")
            if self.casefold:
                line = line.casefold()
            if self.unidecode:
                line = unidecode.unidecode(line)
            if self.remove_trailing_spaces:
                line = line.rstrip()
                print(repr(old), repr(line))
            return line

        return transform


@dataclass
class ManualQuestion(QuestionWithFile):
    """
    Manually assess text files.
    """

    id: QuestionId
    file: Path
    title: str = ""
    description: str = ""


@dataclass
class IoQuestion(QuestionWithFile):
    """
    Question based on IO
    """

    id: str
    question: judge.Question
    file: Path
    timeout: float = 10.0
    title: str = ""
    description: str = ""
    inputs: list[Any] = field(default_factory=list)
    mode: str = "equal"

    def _io_examples_path(self) -> Path:
        path = self.exam.submissions_path / self.id
        return path / "io-examples.json"

    def grade_submission_auto(self, submission: TextSubmission) -> Assessment:
        timeout = self.timeout
        sub = self.question.new_submission(submission.text)
        fb = self.question.check_submission(sub, timeout=timeout)
        return Assessment.from_feedback(fb, key=self.id)

    def prepare(self):
        if not self.inputs:
            return
        for input in self.inputs:
            self.question.add_example(input, kind=self.mode)


# ============================================================================
# Auxiliary functions
# ============================================================================
def _manually_grade_submission(
    question: Question, submission: Submission
) -> Assessment:
    print = config.console.print
    student = submission.student
    print(f"\nGrading [b]{student.id}[/] ({student.name})")
    return _grade_text_manually(submission.text, {})


def _grade_text_manually(text: str, grades: Grades) -> Assessment:
    """
    Display text to user and manually confirm grades.
    """
    from teach.core.cli import ask

    console = config.console

    out = {}
    lines = ["    " + ln for ln in text.splitlines()]
    nlines = len(lines)
    data = "\n".join(lines)
    if nlines > 100:
        console.clear()
        console.print(data)
    else:
        console.print("\n")
        console.print(Syntax(data, "python"))
        console.print("\n")

    console.print(f"[red]Competencies[/] (grades = {grades})")

    while True:
        choices = ["ok", "bad", "grade", "each", "skip"]  # "run",
        action = Prompt.ask(
            "What do you want to do? ",
            choices=choices,
            console=config.console,
            default=choices[0],
        )
        match action:
            case "each":
                for comp, fraction in grades.items():
                    comp_key = f"  * <b><red>{comp}</red></b>"
                    if fraction is False:
                        console.print(f"{comp_key}: disabled")
                        out[comp] = 0
                    elif fraction is True:
                        out[comp] = int(ask(f"{comp_key}: ", console=console))
                    else:
                        cls = type(fraction)
                        out[comp] = cls(console.input(f"{comp_key}: "))
                return Assessment(out, mode=Assessment.MODE_MANUAL)
            case "skip":
                return Assessment({}, mode=Assessment.MODE_SKIP)
            case "ok":
                return Assessment(grades, mode=Assessment.MODE_MANUAL)
            case "bad":
                grades = {key: 0 for key in grades}
                return Assessment(grades, mode=Assessment.MODE_MANUAL)
            case "grade":
                fraction = float(Prompt.ask("Fraction"))
                grades = {key: number(v * fraction) for key, v in grades.items()}  # type: ignore
                return Assessment(grades, mode=Assessment.MODE_MANUAL)
            # case "run":
            #     with tempfile.NamedTemporaryFile("w+", suffix=".py") as fd:
            #         fd.write(text)
            #         fd.flush()
            #         runner = analysis.ScriptRunner.python(fd.name)
            #         runner.run_interactive()


def _io_input_string(input: IoInput) -> str:
    if isinstance(input, (tuple, list)):
        return "\n".join(map(_io_input_string, input))
    return str(input)
