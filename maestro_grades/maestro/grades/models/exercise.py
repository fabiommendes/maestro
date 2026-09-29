from dataclasses import dataclass
from pathlib import Path
from enum import IntEnum

from .base import Model, Grades, CompetencyId, Grade
from ..utils import CHEKIO_EXERCISE_URL, url_normalizer


__all__ = ["Competency", "GradeSpec"]


class Approval(IntEnum):
    REJECTED = 0
    APPROVED = 1
    MERIT = 2


@dataclass
class GradeSpec(Model):
    id: CompetencyId
    min: Grade
    extra: Grade

    REJECTED = Approval.REJECTED
    APPROVED = Approval.APPROVED
    MERIT = Approval.MERIT

    def approval_level(self, grade: Grade) -> Approval:
        """
        Return approval level for given grade.
        """
        return Approval(int(grade >= self.min) + int(grade >= self.extra))

    def progress(self, grade: Grade, max=2.0) -> float:
        """
        A continuous measure of progress from 0 to 2:
        """
        if grade <= self.min:
            return grade / self.min
        elif grade <= self.extra:
            return 1.0 + (max - 1.0) * (grade - self.min) / (self.extra - self.min)
        else:
            return max


@dataclass
class Competency(Model):
    """
    Represents a simple competency.
    """

    id: str
    description: str | None = None
    is_advanced: bool = False

    def __str__(self):
        return self.id


def load_exercises(
    path: Path,
    skip_files=("config.fred",),
    skip_exts=(".log", "-grades.csv"),
    shallow: bool = False,
) -> dict[CompetencyId, Grades]:
    from ..parsers import parse_exercises, render_node

    url_map = url_normalizer("id", CHEKIO_EXERCISE_URL)
    strip_cmt = lambda x: render_node(x).partition("(")[0].strip()
    exercises = {}
    for file in path.iterdir():
        if file.name.endswith(".md"):
            for ex in parse_exercises(
                file.read_text(), url_map, parse_values=(strip_cmt, "eval", float)
            ):
                exercises[ex.id] = ex
            continue
        elif file.is_dir():
            if shallow:
                continue
            out = load_exercises(file, shallow=True)
            if out:
                print("TODO: load exercises", out)
            continue
        elif file.name in skip_files:
            continue
        elif any(file.name.endswith(ext) for ext in skip_exts):
            continue
        elif file.name.endswith(".json"):
            is_valid = False
            for prefix in ["grades-"]:
                if file.name.startswith(prefix):
                    is_valid = True
                    break
            if is_valid:
                continue

        raise ValueError(f"unsupported exercise file: {file.name}")
    return exercises
