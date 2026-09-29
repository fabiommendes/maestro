"""
Project-wide type definitions.
"""

from __future__ import annotations

from pathlib import Path
from typing import NewType

from returns import result

from .calltree import CallTree
from .repo import Repo, RepoError, RepoId, repo_id

__all__ = [
    "ContainerId",
    "Fn",
    "Grade",
    "GradeSet",
    "Loc",
    "QuestionId",
    "repo_id",
    "RepoResponseSet",
    "RepoResult",
    "RepoResults",
    "Repos",
    "ResponseItem",
    "ResponseSet",
    "StudentId",
]

#
# Basic identifiers
#

#: Student identifier.
StudentId = NewType("StudentId", str)

#: Question item identifier. Used to identify questions/sub-questions in the
#: context of an activity.
QuestionId = NewType("QuestionId", str)

#: Identifier for container used in sandboxing
type ContainerId = str

#: Identify a location in a repository. This should be a relative path to
#: some (existing or not ) repository file.
Loc = NewType("Loc", Path)

#: Used as hash values across the system
Hash = NewType("Hash", str)

#
# Grading types
#

#: The numeric grade type
Grade = NewType("Grade", float)

#: A textual type for a student response for a question item. Usually this is
#: associated with free-form questions or some other type of question that
#: requires manual grading.
ResponseItem = NewType("ResponseItem", str)

#: The set of responses for a single question which may contain multiple
#: items.
type ResponseSet = dict[QuestionId, ResponseItem]

#: The grades for a ResponseSet.
type GradeSet = dict[QuestionId, Grade]

#: Collect a buch of RepoResponses for different questions.
type RepoResponseSet = dict[QuestionId, dict[RepoId, ResponseItem]]

#: Collect a buch of RepoResponses for different questions.
type RepoGradeSet = dict[QuestionId, dict[RepoId, Grade]]

#: Pipeline types
type Fn[In, Out] = CallTree[In, Out]

type RepoResult[T] = result.Result[T, RepoError]
type RepoResults[T] = list[RepoResult[T]]
type Repos = list[RepoResult[Repo]]
