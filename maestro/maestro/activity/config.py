from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Literal

import pandas as pd
from pydantic import BaseModel, Field, field_validator, model_validator

from maestro.activity import constants

from ..patterns import Pattern, pattern
from ..types import Grade, QuestionId
from ..utils import map_dicts, max_non_null, min_non_null
from .repository import DuplicateFile, DuplicateResponse

type Duplicate = DuplicateFile | DuplicateResponse
type PlagiarismPolicyItem = (
    PlagiarismQuestionPolicy | PlagiarismFilePolicy | PlagiarismFileToQuestionsPolicy
)
type PlagiarismAction = PlagiarismStaticAction | PlagiarismDiscountAction


class FileConfig(BaseModel):
    """
    Configuration about file roles in a repository.
    """

    exclude: list[str] = Field(default_factory=list)
    protect: list[str] = Field(default_factory=list)
    expect: list[str] = Field(default_factory=list)
    require: list[str] = Field(default_factory=list)

    def exclude_patterns(self) -> Iterable[Pattern]:
        """
        Return a list of glob patterns to exclude from the repository.
        """
        for data in self.exclude:
            match data:
                case "@uv":
                    yield from map(pattern, constants.UV_EXCLUDE_PATTERNS)
                case _:
                    yield pattern(data)

    def protect_patterns(self) -> Iterable[Pattern]:
        """
        Return a list of glob patterns to protect in the repository.
        """
        for data in self.protect:
            match data:
                case "@uv":
                    yield from map(pattern, constants.UV_PROTECTED_PATTERNS)
                case _:
                    yield pattern(data)

    def expect_patterns(self) -> Iterable[Pattern]:
        """
        Return a list of glob patterns that are expected to be present in the repository.
        """
        for data in self.expect:
            yield pattern(data)

    def require_patterns(self) -> Iterable[Pattern]:
        """
        Return a list of glob patterns that are required to be present in the repository.
        """
        for data in self.require:
            yield pattern(data)


class QuestionConfig(BaseModel):
    """
    Configuration about questions in a repository.
    """

    include: list[QuestionId] | None = None
    exclude: list[QuestionId] | None = None
    manual: list[ManualGradingOptions] = Field(default_factory=list)
    weights: dict[QuestionId, Grade] = Field(default_factory=dict)
    merge: dict[QuestionId, Aggregator] = Field(default_factory=dict)

    @field_validator("merge", mode="before")
    @classmethod
    def _merge_can_accept_list_of_files(cls, value: Any) -> Any:
        """
        Validate the merge options.
        """
        if isinstance(value, list):
            return {"method": "sum", "items": value}
        return value

    @model_validator(mode="after")
    def _include_or_exclude(self) -> QuestionConfig:
        """
        Ensure that either include or exclude is set, but not both.
        """
        if self.include is not None and self.exclude is not None:
            raise ValueError("Cannot set both include and exclude in question config.")
        return self

    def merged_weights(self) -> dict[QuestionId, Grade]:
        """
        Return the weights for questions after merging.
        """
        weights = self.weights
        data: dict[QuestionId, Grade] = {}
        default = weights.get(QuestionId("*"), Grade(1.0))

        for id, option in self.merge.items():
            num = weights.get(id, default)
            question_ids = option.items
            denom = sum(
                (weights.get(q, default) for q in question_ids),
                start=Grade(0),
            )
            data[id] = Grade(num / denom) if denom != 0 else Grade(1)
        return data


class Aggregator(BaseModel):
    """
    Aggregate multiple questions into a single value.
    """

    items: list[QuestionId]
    method: Literal["sum", "average", "max", "min", "geometric"] = "sum"
    implicit_zero: bool = True

    @property
    def questions(self) -> list[QuestionId]:
        return self.items

    def __call__[Id](self, data: dict[QuestionId, dict[Id, Grade]]) -> dict[Id, Grade]:
        zero = Grade(0)
        empty = zero if self.implicit_zero else None
        questions = [data[id] for id in self.items if id in data]
        match self.method:
            case "sum":
                return map_dicts(lambda *args: sum(args), *questions, fill=zero)
            # case "average":
            #     total = sum((x for x in xs if x is not None), start=zero)
            #     n = sum(1 for x in xs if x is not None)
            #     return Grade(total / n) if n > 0 else zero
            case "max":
                return map_dicts(max_non_null, *questions, fill=empty)
            case "min":
                return map_dicts(min_non_null, *questions, fill=empty)
            # case "geometric":
            #     product = 1.0
            #     count = 0
            #     for x in xs:
            #         if x is not None:
            #             product *= x
            #             count += 1
            #     if count == 0:
            #         return zero
            #     return Grade(product ** (1 / count))
            case _:
                raise ValueError(f"Unknown merge method: {self.method}")

    def merge_columns(
        self, dataframe: pd.DataFrame, exclude: bool = False
    ) -> list[QuestionId]:
        """
        Merge the columns based on the items in this merge option.
        """
        raise NotImplementedError
        # return [col for col in columns if col in self.items]


class ManualGradingOptions(BaseModel):
    """
    A question that requires manual grading.
    """

    file: Path
    parse: str | None = None
    name: str | None = None
    syntax: str | None = None
    show_id: bool | None = None
    grade_levels: list[str] | None = None

    def sort_grading_options(self) -> dict[str, Any]:
        """
        Return the options for the question.
        """
        options = {
            "syntax": self.syntax,
            "show_id": self.show_id,
            "grade_levels": self.grade_levels,
        }
        return {k: v for k, v in options.items() if v is not None}


#
# Plagiarism detection and handling
#
class PlagiarismOptions(BaseModel):
    """
    Options for the plagiarism detection and subsequent penalties.
    """

    disabled: bool = False
    policies: list[PlagiarismPolicyItem] = Field(default_factory=list)

    def match(
        self, duplicate: DuplicateResponse | DuplicateFile
    ) -> PlagiarismPolicyItem | None:
        """
        Match the duplicate with the policies.
        """
        if isinstance(duplicate, DuplicateFile):
            matches = self._match_duplicate_file(duplicate)
        elif isinstance(duplicate, DuplicateResponse):
            matches = self._match_duplicate_response(duplicate)
        else:
            raise TypeError(f"Unknown duplicate type: {type(duplicate)}")

        match [*matches]:
            case []:
                return None
            case [policy]:
                return policy
            case _:
                raise ValueError(
                    "Multiple policies matched for the duplicate file. "
                    "Please ensure that policies are mutually exclusive."
                )

    def _match_duplicate_file(self, duplicate: DuplicateFile):
        """
        Match the duplicate file with the policies and return the
        corresponding action.
        """
        for policy in self.policies:
            if (
                isinstance(policy, PlagiarismQuestionPolicy)
                or duplicate.file not in policy.files
            ):
                continue
            elif duplicate.hash in policy.accept:
                yield policy.ignoring()
            else:
                yield policy

    def _match_duplicate_response(self, duplicate: DuplicateResponse):
        """
        Match the duplicate response with the policies and return the
        corresponding action.
        """
        for policy in self.policies:
            match policy:
                case PlagiarismQuestionPolicy(questions=questions):
                    if duplicate.question_id in questions:
                        yield policy


class BasePolicy:
    action: PlagiarismAction

    @field_validator("action", mode="before")
    @classmethod
    def _normalize_action_string(cls, value: Any) -> Any:
        if isinstance(value, str):
            return parse_plagiarism_action_string(value)
        return value

    @property
    def ignore(self) -> bool:
        return self.action == PlagiarismStaticAction(kind="ignore")


class PlagiarismQuestionPolicy(BasePolicy, BaseModel):
    """
    A policy for handling plagiarism in a repository.
    """

    questions: list[QuestionId] = Field(default_factory=list, min_length=1)
    action: PlagiarismAction

    def ignoring(self):
        """
        Return a copy of the policy ignoring any action.
        """
        return PlagiarismQuestionPolicy(
            questions=self.questions,
            action=PlagiarismStaticAction(kind="ignore"),
        )


class PlagiarismFileToQuestionsPolicy(BasePolicy, BaseModel):
    """
    A policy for handling plagiarism in a repository.
    """

    action: PlagiarismAction
    files: list[Path] = Field(default_factory=list, min_length=1)
    questions: list[QuestionId] = Field(default_factory=list, min_length=1)
    accept: set[str] = Field(default_factory=set)

    def ignoring(self):
        """
        Return a copy of the policy ignoring any action.
        """
        return PlagiarismFileToQuestionsPolicy(
            action=PlagiarismStaticAction(kind="ignore"),
            files=self.files,
            questions=self.questions,
            accept=self.accept,
        )


class PlagiarismFilePolicy(BasePolicy, BaseModel):
    """
    A policy for handling plagiarism in a repository.
    """

    action: PlagiarismAction
    files: list[Path] = Field(default_factory=list, min_length=1)
    accept: set[str] = Field(default_factory=set)

    def ignoring(self):
        """
        Return a copy of the policy ignoring any action.
        """
        return PlagiarismFilePolicy(
            action=PlagiarismStaticAction(kind="ignore"),
            files=self.files,
            accept=self.accept,
        )


class PlagiarismDiscountAction(BaseModel):
    """
    Action to take when plagiarism is detected.
    """

    kind: Literal["multiplicative", "additive"]
    value: float

    @property
    def is_multiplicative(self) -> bool:
        return self.kind == "multiplicative"

    @property
    def is_additive(self) -> bool:
        return self.kind == "additive"

    def multiplier_for(self, duplicate: Duplicate) -> float:
        """
        Return the value for the given repository ID.
        """
        return self.value if self.kind == "multiplicative" else 1.0

    def increment_for(self, duplicate: Duplicate) -> float:
        """
        Return the value for the given repository ID.
        """
        return self.value if self.kind == "additive" else 0.0


class PlagiarismStaticAction(BaseModel):
    """
    Action to take when plagiarism is detected.
    """

    kind: Literal["ignore", "warn", "fail", "share"]

    @property
    def is_multiplicative(self) -> bool:
        return self.kind in ("fail", "share")

    @property
    def is_additive(self) -> bool:
        return False

    def multiplier_for(self, duplicate: Duplicate) -> float:
        """
        Return the value for the given repository ID.
        """
        match self.kind:
            case "ignore":
                return 1.0
            case "warn":
                return 1.0
            case "fail":
                return 0.0
            case "share":
                return 1 / len(duplicate.ids)
            case _:
                raise ValueError(f"Unknown action kind: {self.kind}")

    def increment_for(self, duplicate: Duplicate) -> float:
        """
        Return the value for the given repository ID.
        """
        return 0.0


#
# Transforms
#
class Transform(BaseModel):
    """
    A transform to apply to the repository.
    """

    name: str
    action: str
    description: str | None = None
    only: Literal["ok", "err", "all"] = "all"


#
# Utility functions
#
def parse_plagiarism_action_string(action: str) -> PlagiarismAction:
    if action in ("ignore", "warn", "fail", "share"):
        return PlagiarismStaticAction(kind=action)  # type: ignore
    raise NotImplementedError
