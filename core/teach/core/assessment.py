from decimal import Decimal
import enum
from fractions import Fraction
from typing import Any, Mapping, Optional
from dataclasses import dataclass, field
from datetime import datetime

from .feedback import Feedback
from .describable import Describable, Details
from .models import JSONMixin


CompetencyId = str
Grade = int | float  # | bool | Fraction | Decimal
Grades = dict[CompetencyId, Grade]


TIMESTAMP_NOT_GIVEN = datetime(1, 1, 1)


class AssessmentMode(enum.Enum):
    """
    Assessment mode - tells how grading occured.
    """

    AUTO = "auto"
    MANUAL = "manual"
    SKIP = "skip"
    ARBITRARY = "arbitrary"


@dataclass(frozen=True)
class Assessment(JSONMixin, Describable):
    """
    Assessment of a competency.

    Attributes:
        grades:
            A grading map from competencies to grades. All grades are scaled between 0 and 1.
        weights:
            An optional map with weights given for each competency. Weights are positive, but
            need not sum to 1.
        feedback:
            Feedback object associated with assessment. Usually this will be present only
            on automatic grading.
        mode:
            Tells how assessment wwas performed and if it was successful. Skipped assessments
            are not considered valid for the final grading.
        comment:
            An arbitrary human friendly comment about the assessment job.
    """

    grades: Grades
    weights: Optional[Grades] = field(default=None, kw_only=True)
    feedback: Optional[Feedback] = field(default=None, kw_only=True)
    mode: AssessmentMode = field(default=AssessmentMode.AUTO, kw_only=True)
    comment: str = field(default="", kw_only=True)
    timestamp: datetime = field(default=TIMESTAMP_NOT_GIVEN, kw_only=True)

    MODE_AUTO = AssessmentMode.AUTO
    MODE_MANUAL = AssessmentMode.MANUAL
    MODE_SKIP = AssessmentMode.SKIP
    MODE_ARBITRARY = AssessmentMode.ARBITRARY

    @property
    def is_skipped(self) -> bool:
        """
        Skipped grades are not counted towards the total.

        This usually flags some kind of error in the grading process.
        """
        return self.mode == AssessmentMode.SKIP

    @property
    def is_valid(self) -> bool:
        """
        A valid assessment must have grades and not be skipped.
        """
        return self.mode != AssessmentMode.SKIP and bool(self.grades)

    @property
    def is_failure(self) -> bool:
        "All grades are equal to zero"

        return all(grade == 0 for grade in self.grades.values())

    @property
    def is_partial(self) -> bool:
        "Some grades are between zero and one"

        if not self.grades:
            return False
        return any(0 < grade < 1 for grade in self.grades.values())

    @property
    def is_success(self) -> bool:
        "All grades are equal to one"

        if not self.grades:
            return False
        return all(grade == 1 for grade in self.grades.values())

    @property
    def mean_progress(self) -> Grade:
        "Weighted mean of all grades"

        if self.weights is None:
            return sum(self.grades.values()) / len(self.grades)

        total = 0.0
        total_w = 0.0
        for k, v in self.grades.items():
            w = self.weights[k]
            total += v * w  # type: ignore
            total_w += w  # type: ignore
        return total / total_w

    MODE_AUTO = AssessmentMode.AUTO
    MODE_SKIP = AssessmentMode.SKIP
    MODE_MANUAL = AssessmentMode.MANUAL
    ARBITRARY = AssessmentMode.ARBITRARY

    @classmethod
    def from_feedback(
        cls, feedback: Feedback, key: CompetencyId, grade: Grade | None = None
    ) -> "Assessment":
        """
        Create an assessment from a feedback.
        """

        if grade is None:
            grade = int(feedback.is_success)
        return cls({key: grade}, feedback=feedback)

    def __post_init__(self):
        if self.timestamp is TIMESTAMP_NOT_GIVEN:
            object.__setattr__(self, "timestamp", datetime.now())

    def __iter_details__(self) -> Details:
        raise NotImplementedError

    def to_dict(self) -> dict:
        data = super().to_dict()
        data["mode"] = self.mode.name  # type: ignore
        return data

    def description(self) -> str:
        if not self.grades:
            return "Failure: no grades given"
        elif self.is_success:
            return "Success!"
        elif self.is_failure:
            return f"Error!"
        elif self.is_partial:
            errors = ", ".join(f"{k}: {v}" for k, v in self.grades.items() if 0 < v < 1)
            return f"Partial success: {errors}"
        raise RuntimeError

    def grade(self, totals: Grade | Grades = 1.0) -> Grades:
        """
        Compute grade re-scaled to the given totals dictionary.
        """
        raise NotImplementedError

    def mean_grade(self, weights: Grade | Grades = 1.0) -> Grades:
        """
        Compute the mean grade re-scaled to the given totals dictionary.
        """
        raise NotImplementedError
