import io
from numbers import Number
from typing import (
    IO,
    Dict,
    Mapping,
    Optional,
    Set,
    Text,
    Tuple,
    TypeVar,
    Literal,
    Union,
)
from .models import Student, Course

N = TypeVar("N", bound=Number)
Competencies = Mapping[str, N]


def report_competencies(grades: Competencies[N], **kwargs) -> str:
    """
    Return string a report for the given list of grades.

    Grades are encoded in a mapping from

    Notes:
        This is just a convenience wrapper around :func:`dump_report`
    """
    with io.StringIO() as fd:
        dump_competencies_report(grades, fd, **kwargs)
        src = fd.getvalue()
    return src


def dump_competencies_report(
    grades: Competencies[N],
    file: IO[Text],
    competencies: Union[Set[str], Mapping[str, str], None] = None,
    min_grade: N = 10,
    student: Optional[Student] = None,
    course: Optional[Course] = None,
    format: Literal["md"] = "md",
) -> None:
    """
    Save report for the given list of grades.

    Args:
        grades:
            A mapping from competence (str) to grade (Number).
        file:
            File
        min_grade:
            Minimum grade necessary to prove compentence.
        competencies:
            An optional set with all valid competencies. Any extra competence
            in grades is ommited and any string present in "competencies" is
            receives an implicit grade of zero.

            This option can also be a mapping from competence to its description.
        format:
            Report format. Supports only markdown ("md"), for now.
        student:
            Information about the student
    """
    grades, competencies = normalize_grades(grades, competencies, min_grade * 0)
    approved = sorted(k for k, v in grades.items() if v >= min_grade)
    missing = {k: v for k, v in grades.items() if v < min_grade}
    kwargs = {
        "approved": approved,
        "missing": missing,
        "competencies": competencies,
        "student": student,
        "discipline": course,
        "min_grade": min_grade,
    }

    if format == "md":
        _dump_report_md(file, **kwargs)
    else:
        raise ValueError(f"invalid format: {format!r}")


def _dump_report_md(
    file, *, competencies, approved, missing, student, discipline, min_grade
):
    n_competencies = len(approved) + len(missing)

    # Title
    if discipline:
        extra = f" ({discipline.name})"
    else:
        extra = ""
    file.write(f"# Relatório de competências{extra}")

    # Student information
    if student:
        file.write("\n\n## Aluno(a)\n\n")
        file.write(student.markdown())

    # Grades
    file.write(f"\n\n## Competências obtidas ({len(approved)}/{n_competencies})\n")
    for comp in approved:
        descr = competencies[comp] or ""
        if descr:
            descr = f": {descr}"
        file.write(f"\n* {comp}{descr}")

    file.write(f"\n\n## Competências incompletas ({len(missing)}/{n_competencies})\n")
    for comp, grade in missing.items():
        descr = competencies[comp] or ""
        if descr:
            descr = f": {descr}"
        file.write(f"\n* {comp} ({grade}/{min_grade}){descr}")


def normalize_grades(
    grades: Competencies[N], competencies: Optional[Set[str]], default: N
) -> Tuple[Dict[str, N], Dict[str, Optional[str]]]:
    """
    Normalize grades and competencies into a uniform representation.

    Return the normalized (grades, competencies) pair.
    """
    if competencies is None:
        grades = grades.copy()
        competencies = {k: None for k in grades}
    else:
        grades = {label: grades.get(label, default) for label in competencies}
        if isinstance(competencies, Mapping):
            competencies = dict(competencies.items())
        else:
            competencies = {k: None for k in competencies}
    return grades, competencies
