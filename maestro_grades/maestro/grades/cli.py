from collections import deque
from functools import partial
import json
from pathlib import Path
from typing import Any, Optional, cast

import pandas as pd
import tt
import ezio
from rich.table import Table
from rich.text import Text

from teach.core.cli import out, err
from teach.core.string import indent


from .render import render_template
from .models import Course, Course
from .loaders import read_transformed
from .models.course import transform_to_progress
from .competencies import dump_competencies_report, report_competencies
from . import parsers
from . import config

__all__ = []
export = lambda fn: __all__.append(fn.__name__) or fn  # type: ignore


@export
def init(name: str = "", slug: str = ""):
    """
    Start a new grading repository.

    Args:
        path: Path to the grading repository.
    """
    path = Path().resolve()
    opts = {"name": name or path.name, "slug": slug or path.name}
    raise NotImplementedError


@export
def info():
    """
    Show information about course.
    """
    course = load_course().load_all()
    data = course.describe()

    # Title
    out.print(f"[b]{data.pop('name')}[/] ({data.pop('id')})")
    if description := course.description:
        out.print()
        out.print(indent(description, 2), soft_wrap=True)
    out.print()

    # Print information about elements of the course
    items: deque[tuple[str, str | Text, Any]]
    items = deque(("  *", k, v) for k, v in data.items())

    while items:
        prefix, k, v = items.popleft()
        if isinstance(k, str):
            field = "[b accent]" + k.replace("_", " ").capitalize() + "[/]"
        else:
            field = k
        out.print(prefix, " ", field, ":", end="", sep="")

        if k == "exams":
            exams: dict = v
            out.print()
            for k, exam in exams.items():
                exam: dict
                n_competencies = len(exam.pop("competencies"))
                name = Text(exam.pop("id"), style="b u yellow")
                items.append(("    *", name, exam.pop("title")))
                items.extend(("    -", k, exam) for k, exam in exam.items())
                items.append(("    -", "competencies", f"{n_competencies}\n"))
            continue
        out.print("", v)
    out.print()


@export
def students(email: bool = False, send_to: bool = False, sep: str = "\n"):
    """
    Show information about students.

    Args:
        email:
            Show all email addresses.
        send_to:
            Show e-mails in the form of <Student Name e-mail@example.com>

    If neither `email` nor `send_to` is specified, the default is to show
    a table with student information.
    """
    course = load_course()
    df = pd.DataFrame.from_records(s.__dict__ for s in course.students.values())

    if email:
        for email in sorted({st.strip() for st in df["email"].dropna()}):
            out.print(email, end=sep)

    elif send_to:
        for row in df[["name", "email", "is_active"]].itertuples():
            if row.is_active:
                out.print(f"<{row.name} {row.email}>", end=sep)

    else:
        df = df.sort_values("name")
        df["is_active"] = df["is_active"].apply(lambda x: "✅" if x else "⛔")
        table = Table(*df.columns)
        for _, row in df.iterrows():
            table.add_row(*map(str, row.values))
        out.print(table)


@export
def show_competencies():
    """
    Show information about course.
    """
    course = load_course()
    df = pd.DataFrame.from_records(s.__dict__ for s in course.competencies.values())
    df = df.sort_values("id")
    table = Table("ID", "Description")
    for _, row in df.iterrows():
        color = "b" if row["is_advanced"] else "green"
        ref = f"[{color}]{row['id']}[/]"
        table.add_row(ref, row["description"])
    config.console.print(table)


@export
def grade_exam(name: str, force: bool = False, competencies: bool = False):
    """
    Grade a given exam.
    """
    course = load_course()
    exam = course.get_exam(name, load=True)
    grades_data = exam.grade_questions(force=force)
    competencies_data = exam.transform_grades(grades_data)

    df_grades = pd.DataFrame.from_records(grades_data).T
    df_grades.index.name = "id"

    if len(df_grades) == 0:
        out.print("No grades to export.")
        return

    # Print files
    path = exam.path.joinpath("final-grades.csv").resolve()
    df_grades.to_csv(path)

    path = exam.path.joinpath("final-competencies.csv").resolve()
    df_competencies = pd.DataFrame.from_records(competencies_data).T
    df_competencies.index.name = "id"
    df_competencies.to_csv(path)

    # Show statistics
    df = df_competencies if competencies else df_grades
    out.print("\n[b red]Exam statistics[/]")
    out.print(df.agg(["mean", "median", "max"]).T.sort_index())  # type: ignore


@export
def report(id: str = "", show_info: bool = False):
    """
    Update student reports
    """
    course = load_course()
    data = course.collect_grades(with_totals=True).to_dict("index")
    competencies = set()
    for v in data.values():
        competencies.update(v)

    if id:
        grades = data.get(id)
        student = course.get_student(id)
        report = report_competencies(
            grades, student=student, course=course, competencies=competencies
        )
        print(report)
        return

    for id, student in course.students.items():
        grades = data.get(id, {})
        grades.pop("id", None)
        kwargs = {"course": course, "competencies": competencies}
        if show_info:
            kwargs["student"] = student

        report_path = course.reports_path.joinpath(f"{student.slug}.md")
        with open(report_path, "w") as fd:
            dump_competencies_report(grades, fd, **kwargs)


@export
def collect(
    output: Optional[Path] = None,
    summary: bool = False,
    by_grade: bool = False,
    exams: str = "",
    progress: bool = False,
    name: bool = False,
):
    """
    Collect all grades into a single dataframe.
    """
    course = load_course()
    only = exams.split(",") if exams else None
    data = course.collect_grades(with_totals=True, only=only)
    if by_grade:
        data.sort_values("total", inplace=True, ascending=False)

    # Reorganize data
    if summary:
        name = False
        table = _summary(data, course, progress, by_grade)
    elif progress:
        table = transform_to_progress(data, course.grades)
    else:
        table = data

    # Print or save
    if name:
        cols = table.columns
        table["name"] = [course.students[id].name for id in table.index]
        table = table[["name", *cols]]
        if not by_grade:
            table.sort_values("name", inplace=True)
    if output:
        tt.save_table(table, str(output))
    else:
        out.print(table)


def _summary(data, course, progress, by_grade):
    # Compute statistics
    data = data.drop(columns=["total", "level"])
    data = transform_to_progress(data, course.grades) if progress else data
    df = data.agg(["mean", "median", "max"]).T.sort_index()  # type: ignore

    # Add progress levels for questions
    levels = [0.2, 0.5, 0.7, 1.0] if progress else [3, 5, 7, 10]
    for level in levels:
        df[f"{level}+"] = (data >= level).sum(axis=0)

    # Order
    if by_grade:
        df.sort_values("mean", inplace=True, ascending=False)

    return data


@export
def sigaa_script(path: str, column: str = "level"):
    """
    Generate a script to import grades into Sigaa.

    The script must be copied and paste in the browser console for the grading form.
    An ugly hack, that works for me :)
    """
    # course = load_course()
    # data = course.collect_grades(with_totals=True)["level"]
    data = tt.read_table(path)[column]
    json_data = json.dumps(data.to_dict())
    print(render_template("sigaa-grades.js", {"grades": json_data}))


@export
def transform(name: str, path: Path = Path(), verbose: bool = False):
    """
    Transform names
    """
    repo = load_course()
    data = read_transformed(repo, name, path / "grades", verbose=verbose)
    config.console.print(data)


#
# Parsers
#
@export
def list_competencies(path: Path):
    """
    Parse a list of competence files.
    """
    src = path.read_text()
    for comp in parsers.parse_competencies(src):
        medal = " (🏅)" if comp.is_advanced else ""
        ezio.print(f"<green>#</green> <b>{comp.id}</b>{medal}", format=True)
        description = comp.description or "No description"
        ezio.print()
        ezio.print(description, max_width=60, indent=2)
        ezio.print()


def load_course(load_all=False) -> Course:
    """
    Load repository at given path.
    """
    conf = Path("maestro.conf")
    if not conf.exists():
        err.print("Is this a valid [b]Maestro[/] repository?\n")
        err.print("💩💩💩 Could not find the [b red]maestro.conf[/] file. 💩💩💩")
        raise SystemExit(1)

    course = Course.load_file(conf)
    if load_all:
        course.load_all()

    try:
        course.validate("all")
    except ValueError as ex:
        err.print("💩💩💩 Invalid course configuration. 💩💩💩")
        raise SystemExit(str(ex))
    return course


def star_render_cell(spec, x, emoji=False):
    if emoji:
        match spec.approval_level(x):
            case spec.REJECTED:
                return "🚫"
            case spec.APPROVED:
                return "⭐"
            case spec.MERIT:
                return "⭐⭐"
    else:
        match spec.approval_level(x):
            case spec.REJECTED:
                return "-"
            case spec.APPROVED:
                return "y"
            case spec.MERIT:
                return "yy"
