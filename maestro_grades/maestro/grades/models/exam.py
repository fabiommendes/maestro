from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import datetime
from dataclasses import dataclass, field
from functools import wraps
from itertools import chain
import operator
from pathlib import Path
from typing import (
    Any,
    Callable,
    Iterable,
    Mapping,
    NamedTuple,
    TypeVar,
    TYPE_CHECKING,
)

import pandas as pd
import tt
import fred

from teach.core.fred import FredTransformer, role
from teach.core import Model, ParentMixin, CompetencyId, Grade
from teach.core.string import humanize
from teach.core.cli import out

from .base import (
    GradesDb,
    QuestionId,
    QuestionRef,
    StudentId,
    StudentRef,
    Grades,
    DataMapping,
    get_console,
)
from .question import (
    Question,
    LinePermQuestion,
    ManualQuestion,
    EmptyQuestion,
    IoQuestion,
)
from .log import JSONLog
from .student import Student
from .submission import Submission
from ..config import log, ctx
from ..utils import number, exercise_url_normalizer
from ..loaders import read_file

if TYPE_CHECKING:
    from .course import Course

__all__ = ["Exam"]

T = TypeVar("T")
M = TypeVar("M", bound="Model")


@dataclass
class Exam(Model, ParentMixin, DataMapping[Question]):
    """
    A exam is an ordered collection of exercises.

    It functions as a mapping from exercise id's and exercise objects.
    """

    id: str
    path: Path
    title: str = ""
    description: str = ""
    student_id_field: str = "id"
    competencies: dict[str, Grades] = field(default_factory=dict)
    questions: dict[str, Question] = field(default_factory=dict)
    deadline: datetime | None = None

    # Derived properties
    data = property(lambda self: self.questions)  # type: ignore

    @property
    def questions_path(self) -> Path:
        return self.path / "questions"

    @property
    def submissions_path(self) -> Path:
        return self.path / "submissions"

    @property
    def tmp_path(self) -> Path:
        return self.path / "tmp"

    @property
    def course(self) -> "Course":
        course = self.parent
        if course is None:
            raise RuntimeError("Exam is not part of a course")
        return course

    @course.setter
    def course(self, course: "Course"):
        self.parent = course

    @classmethod
    def fred_loader(cls, path: Path) -> Any:
        return DeserializerTransformer(path.parent)(fred.load(path))

    def describe(self) -> dict:
        """
        Compiles a description of the exam.
        """
        competencies = sorted(set(chain(*self.competencies.values())))
        return {
            "id": self.id,
            "description": self.description or "No description",
            "title": self.description or humanize(self.id).title(),
            "questions": len(self.questions),
            "competencies": competencies,
            # "submissions": sum(1 for _ in self.iter_submissions()),
        }

    @contextmanager
    def assessment_log(self):
        """
        Return a log object.
        """
        path = self.path.joinpath("assessment.log")
        if not path.exists():
            path.write_text("")
        try:
            yield JSONLog(path)
        finally:
            pass

    def base_grades(self) -> GradesDb:
        """
        Base grades are given manually or may consist on arbirary fixes
        applied to specific students.
        """
        try:
            df = read_grade_dataframe(self.path, "base-grades")
        except FileNotFoundError:
            return {}
        else:
            return grades_dataframe_to_db(df, self.course)

    def extra_grades(self) -> GradesDb:
        """
        Extra values given to students ad-hoc.
        """
        try:
            df = read_grade_dataframe(self.path, "extra-grades")
        except FileNotFoundError:
            return {}
        else:
            return grades_dataframe_to_db(df, self.course)

    def grade_questions(self, **kwargs) -> GradesDb:
        """
        Grade all questions in exam.
        """
        grades = [grade_question(qst, self) for qst in self.iter_questions()]
        grades.insert(0, self.base_grades())
        grades.append(self.extra_grades())
        return flatten_grades_db(grades)

    def grade_question(
        self,
        question: QuestionRef,
        force=False,
        method=Question.TryAuto,
    ) -> "AssesmentResult":
        """
        Grade a single question, if necesssary.
        """
        skipped = set()
        grades = self.read_grades_from_log(question)
        question = self.get_question(question)
        question.prepare()

        for submission in self.iter_submissions(question):
            student_id = submission.student.id
            if not force and student_id in grades:
                continue

            assessment = question.grade_submission(submission, method)
            if assessment.is_skipped:
                skipped.add(student_id)
            else:
                data: dict
                data = assessment.to_dict()  # type: ignore
                entry = {"question": question.id, "student": student_id}
                entry["feedback"] = data.pop("feedback", {})
                entry.update(data)
                with self.assessment_log() as log:
                    log.add_entry(entry)
                grades[student_id] = assessment.grades

        return AssesmentResult(skipped, grades)

    def question_requires_grading(self, question: QuestionRef) -> bool:
        """
        Return True if any submission for question requires grading.
        """
        question_id = self.get_question(question).id
        students = {sub.student_id for sub in self.iter_submissions(question)}
        with self.assessment_log() as log:
            for entry in log:
                if entry["question"] == question_id:
                    students.discard(entry["student"])
                if not students:
                    return False
        return bool(students)

    def submission_requires_grading(
        self, question: QuestionRef, student: StudentRef
    ) -> bool:
        """
        Return True if submission requires grading.
        """
        question_id = self.get_question(question).id
        try:
            student_id: str = self.get_student(student).id
        except (KeyError, Student.Inactive):
            return False

        with self.assessment_log() as log:
            for entry in log:
                if entry["question"] == question_id and entry["student"] == student_id:
                    return False
        return True

    def read_grades_from_log(self, question: QuestionRef) -> dict[StudentId, Grades]:
        """
        Return a dictionary of grades
        """

        data = {}
        question_id = self.get_question(question).id
        with self.assessment_log() as log:
            for entry in log:
                if entry["question"] == question_id:
                    data[entry["student"]] = entry["grades"]
        return data

    def transform_grades(
        self,
        grades_db: dict[StudentId, Grades],
        acc: Callable[[Grade, Grade], Grade] = operator.add,
    ) -> dict[StudentId, Grades]:
        """
        Transform raw grades to course competencies.
        """
        out_db = {}
        competencies = self.competencies
        missing_competencies: Counter[QuestionId] = Counter()

        for student_id, raw in grades_db.items():
            grades: dict[CompetencyId, int] = defaultdict(int)

            for question_id, grade in raw.items():
                try:
                    question_competencies = competencies[question_id]
                except KeyError:
                    if grade:
                        missing_competencies[question_id] += 1
                    continue

                for competency, points in question_competencies.items():
                    curr_grade = grades[competency]
                    grades[competency] = acc(curr_grade, number(grade * points))  # type: ignore

            out_db[student_id] = dict(grades)

        if missing_competencies:
            out.print("[red b]Missing competencies[/]")
        for question_id, count in missing_competencies.most_common():
            out.print(f"  * [b blue]{question_id}[/]: {count} submissions")
        return out_db

    def iter_questions(self):
        """
        Iterate over all exam questions.
        """
        for question in self.questions.values():
            assert question.parent is self
            yield question

    def iter_submissions(self, question=None) -> Iterable[Submission]:
        """
        Iterate over all submissions, possibly filtering by question/question.id
        """
        path = self.submissions_path.resolve()
        if not path.exists():
            log.warn("Submission directory does not exit")
            return

        if question is None:
            for question in self.iter_questions():
                yield from self.iter_submissions(question)

        for path in iter_submissions_on_snapshots(path):
            # Batch submission: all data is in a single file
            if not path.is_dir():
                data = read_file(path)
                raise NotImplementedError()

            # Each submission is a separate directory
            try:
                student = self.get_student(path.name)
            except Student.Inactive:
                continue
            except KeyError as ex:
                k, v = ex.args[0].popitem()
                ctx.trigger("student.missing", f"{k}={v}")
                continue

            submission = load_submission_from_path(path, student, question)
            if submission is not None:
                yield submission

    def _iter_all_submissions(self, path: Path) -> Iterable[Submission]:
        raise NotImplementedError

    def get_student(self, ref: StudentRef) -> "Student":
        """
        Return student using the id field configured in self.id_field
        """
        if isinstance(ref, Student):
            return ref
        if self.student_id_field == "id":
            args = [ref]
            kwargs = {}
        else:
            args = []
            kwargs = {self.student_id_field: ref}
        return self.course.get_student(*args, **kwargs)  # type: ignore

    def get_question(self, ref: QuestionRef) -> Question:
        """
        Return question from id.
        """
        if isinstance(ref, Question):
            return ref
        elif isinstance(ref, str):
            return self.questions[ref]
        else:
            typ = type(ref).__name__
            raise TypeError(f"invalid question reference: {typ}")


# ============================================================================
# Serialization and deserialization
# ============================================================================
def forward_retval(fn):
    out = fn.__annotations__["return"]

    @wraps(fn)
    def decorated(self, tag, attrs, data):
        if isinstance(data, out):
            return data
        elif not isinstance(data, dict):
            raise TypeError(type(data))

        return fn(self, tag, attrs, data)

    return decorated


class DeserializerTransformer(FredTransformer):
    """
    Deserialize config.fred files that hold exam configurations.
    """

    def __init__(self, path: Path):
        super().__init__(casefold=True, strict=False)
        self._path: Path = path
        self._exam: Exam | None = None
        self._id = "?"

    @role(visit=False)
    @forward_retval
    def exam(self, tag, attrs, data: dict) -> Exam:
        # Defer initialization
        questions = data.pop("questions", {})
        competencies = data.pop("competencies", {})

        # Normalize arguments
        data.setdefault("id", self._path.name)
        data.setdefault("title", humanize(data["id"]))
        data["student_id_field"] = data.pop("student_id", "id")
        data["path"] = self._path
        data = {k: self(v) for k, v in data.items()}

        self._exam = exam = Exam(**data)

        # Initialize questions
        for id, args in questions.items():
            self._id = id
            if args is None:
                question = EmptyQuestion(id=id)
            else:
                question = self(args)
            if not isinstance(question, Question):
                raise TypeError(f"not a question: {question}")
            question.parent = exam
            exam.questions[id] = question

        # Initialize competencies
        if isinstance(competencies, (str, Path)):
            competencies = self._competencies(exam, Path(competencies))

        for id, args in competencies.items():
            exam.competencies[id] = self(args)

        return exam

    @forward_retval
    def manual(self, tag, attrs, data: dict) -> ManualQuestion:
        cons = self._prepare_question(ManualQuestion, data, "file")
        return cons(**self(data))

    @forward_retval
    def lineperm(self, tag, attrs, data: dict) -> LinePermQuestion:
        cons = self._prepare_question(LinePermQuestion, data, "file")
        return cons(**self(data))

    @forward_retval
    def io(self, tag, attrs, data: dict) -> IoQuestion:
        cons = self._prepare_question(IoQuestion, data, "file")
        print(data.pop("check", None))
        print(data.pop("confirm", None))
        print(data)
        return cons(**self(data))

    def _prepare_question(self, cls, data, field="file"):
        # A few sanity checks
        if isinstance(data, str):
            data = {field: data}
            if field is None:
                raise ValueError("expect a dictionary, got string")
        if not isinstance(data, dict):
            raise ValueError(f"expect a dictionary, got {type(data)}")

        fields = cls.__dataclass_fields__

        # Initialize question if it defines a question: QType field.
        # Question must have a constructor QType.from_path that initializes
        # the question completely from the given path
        try:
            mk_question = fields["question"].type.from_path
            file = data["file"]
        except (KeyError, AttributeError) as exc:
            mk_question = None
            file = data.get("file", None)

        if mk_question:
            if file is None:
                raise ValueError("file field was not defined")
            if self._exam is None:
                raise RuntimeError("exam not available")
            path = self._question_file(file)
            data["question"] = mk_question(path)

        # Initialize common arguments for all questions
        data.setdefault("id", self._id)
        data.setdefault("title", humanize(data["id"]))
        memo = data.pop("memo", True)
        self._id = data["id"]

        def cons(**kwargs):
            out = cls(**kwargs)
            if memo and memo in fields:
                out.memo = {}
            return out

        return cons

    def _question_file(self, file: str) -> Path:
        if self._exam is None:
            return Path(file)

        # File path. Seach in order:
        #   1. exam / questions / <id> / <file>
        path = (self._exam.questions_path / self._id / file).resolve()
        if path.exists():
            return path

        #   2. exam / questions / <file>
        path = (self._exam.questions_path / file).resolve()
        if path.exists():
            return path

        #   3. exam / <file>
        path = (self._exam.path / file).resolve()
        if path.exists():
            return path

        raise FileNotFoundError(path)

    def _competencies(self, exam: Exam, path: Path) -> dict:
        from ..parsers import parse_exercises

        path = exam.path.joinpath(path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"no competency file at {path}")

        match path.suffix:
            case ".md":
                src = path.read_text()
                map_id = exercise_url_normalizer
                comp_grades = parse_exercises(src, map_id=map_id)  # type: ignore
                return {comp_id: grades for comp_id, _, grades in comp_grades}
            case _:
                raise ValueError(f"invalid competency file type: {path.name}")


# ============================================================================
# Utilities
# ============================================================================
class AssesmentResult(NamedTuple):
    skipped: set[QuestionId]
    grades: dict[StudentId, Grades]


def normalize_id_set(obj) -> set[str]:
    if obj is None:
        return set()
    if isinstance(obj, str):
        return {obj}
    elif hasattr(obj, "id"):
        return normalize_id_set(obj.id)
    elif isinstance(obj, (tuple, list, set)):
        return set(chain.from_iterable(map(normalize_id_set, obj)))
    else:
        raise TypeError(type(obj))


def grades_dataframe_to_db(grades: pd.DataFrame, course: "Course") -> GradesDb:
    """
    Transform a dataframe with grades to a mapping from student
    to a grades dictionary.
    """
    assert isinstance(grades, pd.DataFrame)

    def get_student_id(value, col):
        if col == "id":
            return course.get_student(value).id
        else:
            return course.get_student(**{col: value}).id

    id_field = grades.index.name or "id"
    normalized = {}
    for (id, row) in grades.iterrows():
        try:
            key = get_student_id(id, id_field)
        except KeyError:
            out.log(f"[red b]Student not found:[/] {id_field} = {id}")
            continue

        normalized[key] = row.to_dict()
    return normalized


def flatten_grades_db(seq: Iterable[GradesDb], agg=lambda x, y: x + y) -> GradesDb:
    """
    Flatten a sequence of various grade dbs into a dictionary.
    """
    seq = iter(seq)
    try:
        acc = next(seq)
    except StopIteration:
        acc = {}

    for part in seq:
        for student_id, grades in part.items():
            grades_acc = acc.setdefault(student_id, {})
            for comp_id, grade in grades.items():
                grades_acc[comp_id] = agg(number(grades_acc.get(comp_id, 0)), grade)
    return acc


def iter_submissions_on_snapshots(path: Path):
    """
    Iterate over all snapshots folders in order of creation.

    Consider the following directory structure:

       - submissions
       |-- snapshot-1
       |  |-- subA
       |  |-- ...
       |  \-- subZ
       |-- snapshot-2
       |  |-- subA
       |  |-- ...
       |  \-- subZ
       \-- snapshot-3
          |-- subA
          |-- ...
          \-- subZ

    The algorithm walks alphabetically on snapshots and iterate over each submission
    on each snapshot in order.

    If no snapshot folder is found, it assumes that the directory contains a series
    of submissions.
    """
    paths = sorted(
        path.iterdir(), key=lambda p: (not p.name.startswith("snapshot-"), p.name)
    )
    for sub in paths:
        if sub.name.startswith("snapshot-"):
            if sub.is_dir():
                yield from sub.iterdir()
            else:
                yield sub
        else:
            yield sub


def load_submission_from_path(
    path: Path, student: Student, question: Question
) -> Submission | None:
    """
    Read a submission contained in a submission directory.
    """
    file = getattr(question, "file", None)
    if file is None:
        return None

    path = path.joinpath(file).resolve()
    if not path.exists():
        log.warn(f"file {path} does not exist")
        return None

    return question.submission_class.parse_obj(path, student, question)


def grade_question(question: Question, exam: Exam) -> GradesDb:
    """
    Grade given question, asking input, if necessary.
    """
    from teach.core.cli import ask

    show = get_console().print

    if not exam.question_requires_grading(question):
        show(f"GRADED EXERCISE [b green]{question.id.upper()}[/]: ", end="")
        show("[red]skipped[/]" if not question.is_active else "[b]complete[/]")
        return exam.read_grades_from_log(question)

    show(f"GRADING EXERCISE [b green]{question.id.upper()}[/]")
    skipped, question_grades = exam.grade_question(question)

    if not skipped:
        show("[b]Congratulations![/] All exercises were graded")
        return question_grades

    if ask(f"{len(skipped)} submissions were skipped, continue?"):
        return question_grades

    return grade_question(question, exam)


def read_grade_dataframe(path: Path, name: str) -> pd.DataFrame:
    """
    Base grades are given manually or may consist on arbirary fixes
    applied to specific students.
    """
    path = path.resolve()
    for ext in [".csv", ".xlsx", ".xls"]:
        if (file := path.joinpath(name + ext)).exists():
            # Read table and convert index to string, since it may
            # be coerced to a numerical value in some cases.
            df = tt.read_table(str(file)).fillna(0)
            df.index = df.index.astype(str)
            return df

    raise FileNotFoundError
