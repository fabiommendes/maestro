from typing import Any, Iterable, Iterator, Sequence, overload
from pathlib import Path
from dataclasses import dataclass, field

import pandas as pd
import fred

from teach.core.fred import FredTransformer

from .base import ExamId, Model, Grade, GradesDb
from .exercise import Competency, GradeSpec
from .exam import Exam, grades_dataframe_to_db, flatten_grades_db
from .utils import clean_empty
from .student import Student
from .. import config
from ..config import ctx
from ..loaders import read_file
from ..transforms import transform_maestro_step

__all__ = ["Course"]


@dataclass
class Course(Model):
    """
    Represents a classroom
    """

    id: str
    name: str
    exams_path: Path = Path("exams")
    reports_path: Path = Path("reports")
    description: str | None = None
    students: dict[str, Student] = field(default_factory=dict, repr=False)
    competencies: dict[str, Competency] = field(default_factory=dict, repr=False)
    exams: dict[str, Exam] = field(default_factory=dict, repr=False)
    grades: dict[str, GradeSpec] = field(default_factory=dict, repr=False)
    levels: dict[str, Grade] = field(default_factory=dict, repr=False)
    fractional_grades: bool = False

    @classmethod
    def fred_loader(cls, path: Path):
        return CourseFredTransformer(path.parent)(fred.load(path))

    def describe(self, humanize=False) -> dict[str, Any]:
        """
        Return a dictionary with information about classroom.
        """
        n_students = len(self.students)
        n_students_active = sum(1 for s in self.students.values() if s.is_active)
        data = {
            "name": self.name,
            "id": self.id,
            "students": n_students_active,
            "inactive_students": n_students - n_students_active,
            "competencies": len(self.competencies),
            "exams": {id: exam.describe() for id, exam in self.exams.items()},
        }
        return data

    def save(self, overwrite=False, path=None):
        """
        Init repository from current course configuration.
        """
        raise NotImplementedError

    @overload
    def get_student(self, id: str, /, *, inactive: bool = False) -> Student:
        ...

    @overload
    def get_student(self, *, github_id: str, inactive: bool = False) -> Student:
        ...

    @overload
    def get_student(self, *, checkio_id: str, inactive: bool = False) -> Student:
        ...

    def get_student(self, *args, inactive=False, **kwargs):
        """
        Return student with given id, github_id, checkio_id, etc.

        Raise KeyError if student is not found.
        """
        if args and kwargs:
            raise TypeError("cannot mix positional and keyword argument/s")
        if len(kwargs) >= 2:
            raise TypeError("only a single keyword argument is allowed")
        if len(args) >= 2:
            raise TypeError("only a single positional argument is allowed")

        if "id" in kwargs:
            args, kwargs = (kwargs["id"],), {}

        if args:
            attr, value = "id", args[0]
            student = self.students.get(value)
        else:
            attr, value = kwargs.popitem()
            if attr not in Student._EXTERNAL_ACCOUNTS:
                argname, _ = kwargs.popitem()
                raise TypeError(f"invalid argument: {argname}")

            student = None
            for student in self.students.values():
                if getattr(student, attr) == value:
                    student = student
                    break

        if student is None:
            raise KeyError({attr: value})
        elif not student.is_active and not inactive:
            raise Student.Inactive({attr: value})
        else:
            return student

    def get_exam(self, exam_id: ExamId, load=True, **kwargs) -> "Exam":
        """
        Fetch exam.

        If load=True, fetch exam from filesystem, if not present.
        """
        try:
            return self.exams[exam_id]
        except KeyError:
            if not load:
                raise

        path = (self.exams_path / exam_id).resolve()
        config_path = path / "config.fred"
        if config_path.exists():
            exam = Exam.load_file(config_path, id=exam_id)
        elif path.exists():
            # Empty folders qualify as valid, but empty exams
            exam = Exam(id=exam_id, path=path)
        else:
            # Folder do not exist, raise error!
            raise KeyError(exam_id)

        exam.parent = self
        self.exams[exam_id] = exam
        return exam

    def collect_grades(
        self,
        *,
        only: Sequence[ExamId] | None = None,
        with_totals: bool = False,
        by_progress: bool = False,
    ) -> pd.DataFrame:
        """
        Collect all grades from repository into a single dataframe..
        """

        data = flatten_grades_db(self._iter_grades(only))
        df = pd.DataFrame.from_records([*data.values()], index=data.keys())
        df = df.fillna(0)
        df = transform_maestro_step(self, df, "clean", {}, script=Path())

        if with_totals:
            extra = compute_grade_levels(
                df, self.grades, self.levels, fractional=self.fractional_grades
            )
            if by_progress:
                df = transform_to_progress(df, self.grades)
            df["total"] = extra["total"]
            df["level"] = extra["level"]
        elif by_progress:
            df = transform_to_progress(df, self.grades)

        return df.sort_index()

    def _iter_grades(self, filter_exams) -> Iterator[GradesDb]:
        for exam_path in self.exams_path.iterdir():
            if not exam_path.is_dir():
                continue

            if filter_exams is not None and exam_path.name not in filter_exams:
                continue

            path = (exam_path / "final-competencies.csv").resolve()
            if not path.exists():
                config.console.log(f"missing grades? [yellow]{exam_path.name}[/]")
                continue

            df = read_file(path, dtype={"id": str}, index_col="id")
            data = grades_dataframe_to_db(df, self)
            yield data

    def transform_by(self, data: pd.DataFrame, script: Path) -> pd.DataFrame:
        """
        Transform dataframe by script in the given path.

        Args:
            data:
                Source dataframe
            script:
                Path to a Python or tt script.
        """
        from .. import transforms

        ext = script.name.rpartition(".")[-1]
        if ext == "py":
            src = script.read_text()
            return transforms.transform_python(self, data, src, script)
        elif ext == "tt":
            src = script.read_text()
            return transforms.transform_tt(self, data, src, script)
        elif ext == "transform":
            src = script.read_text()
            return transforms.transform_maestro(self, data, src, script)
        else:
            raise ValueError(f"invalid script extension: .{ext}")

    def load_exams(self, **kwargs) -> "Course":
        """
        Load all exams from file structure.
        """
        for path in self.exams_path.iterdir():
            if path.is_dir():
                self.get_exam(path.name, load=True, **kwargs)
        return self

    def load_all(self) -> "Course":
        """
        Load all data from repository.
        """
        return self.load_exams()


class CourseFredTransformer(FredTransformer):
    DTYPES = {
        "id": "string",
        "name": "string",
        "email": "string",
        "github_id": "string",
        "checkio_id": "string",
        "beecrowd_id": "string",
        "is_active": bool,
    }

    def __init__(self, path: Path):
        self.path = path
        super().__init__(casefold=True, strict=True)

    def course(self, tag, attrs, data):
        from ..parsers import parse_competencies

        # Normalize paths
        cfg: dict[str, str] = data.pop("paths", {})
        base = self.path.parent
        roster_path = base / cfg.pop("roster", "roster.csv")
        competencies_path = base / cfg.pop("competencies", "competencies.md")
        data["exams_path"] = (base / cfg.pop("exams", "exams")).resolve()
        data["reports_path"] = (base / cfg.pop("reports", "reports")).resolve()
        if cfg:
            raise ValueError(f"invalid config: paths.{next(iter(cfg))}")

        grades = data.pop("grades", {})
        new = Course(**data)

        # Load students
        roster_df = read_file(roster_path.resolve(), dtype=self.DTYPES)
        roster_df.dropna(subset=["id"], inplace=True)
        roster_df["is_active"].fillna(True, inplace=True)
        roster_df.replace(pd.NA, None, inplace=True)
        new.students.update((s.id, s) for s in load_students_from_roster(roster_df))

        # Load competencies
        competencies = parse_competencies(competencies_path.resolve().read_text())
        new.competencies.update((c.id, c) for c in competencies)

        # Load grading configuration
        for id, opts in grades.items():
            opts["id"] = id
            opts.setdefault("min", min(10, opts.get("extra", 10)))
            new.grades[id] = GradeSpec(**opts)

        return new


def load_students_from_roster(
    roster: pd.DataFrame, keep_inactive=False
) -> Iterable["Student"]:
    """
    Load students from database.
    """
    student_fields: set[str] = set(Student.__dataclass_fields__)
    inactive_index = 1

    for _, row in roster.iterrows():
        row = row.to_dict()

        is_active = row.pop("is_active", True)
        if isinstance(is_active, str):
            is_active = is_active.lower() in ("true", "yes", "ok")

        data = {k: v for k, v in row.items() if k in student_fields}
        data = clean_empty(data)
        if not data.get("id") or not data.get("name"):
            is_active = False
        data["is_active"] = is_active

        if not is_active:
            for field in ["id", "name"]:
                if field not in data:
                    data[field] = f"{field}:{inactive_index}"
            inactive_index += 1
        data["id"] = str(data["id"])

        student = Student(**data)
        if keep_inactive or student.is_active:
            yield student


def compute_grade_levels(
    df: pd.DataFrame,
    grades: dict[str, GradeSpec],
    levels: dict[str, Grade],
    fractional: bool = False,
) -> pd.DataFrame:
    """
    Compute grade levels from grades.
    """
    score_levels = sorted(levels.items(), key=lambda x: x[1])
    totals = []
    scores = []

    for _, row in df.iterrows():
        data = row.to_dict()
        total = 0
        for k, v in data.items():
            try:
                if fractional:
                    total += grades[k].progress(v)
                else:
                    total += grades[k].approval_level(v)
            except KeyError:
                ctx.trigger("missing-competency", k)

        score = score_levels[0][0]
        for score_, min_value in score_levels:
            if total >= min_value:
                score = score_
            else:
                break

        totals.append(total)
        scores.append(score)

    return pd.DataFrame({"total": totals, "level": scores}, index=df.index)


def transform_to_progress(
    df: pd.DataFrame, grades: dict[str, GradeSpec]
) -> pd.DataFrame:
    """
    Transform grades in dataframe to progress ratios.
    """

    def to_progress(x):
        if x >= spec.extra:
            return 2.0
        return min(1.0, x / spec.min)

    df = df.copy()
    for k, v in df.items():
        k = str(k)
        try:
            spec = grades[k]
        except KeyError:
            continue
        df[k] = df[k].apply(to_progress)
    return df
