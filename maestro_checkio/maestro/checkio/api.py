from collections import defaultdict
import requests
from datetime import date, datetime
from typing import List, NamedTuple, Dict, Optional, Set, TYPE_CHECKING, Union
import enum
import pandas as pd
from rich.console import Console

console = Console()

if TYPE_CHECKING:
    import pandas as pd

API_URL = "https://py.checkio.org/api"
GROUP_DETAILS = "group-details"
GROUP_PROGRESS = "group-progress"


class ProgressStatus(enum.Enum):
    PUBLISHED = "published"
    TRIED = "tried"
    OPENED = "opened"
    NEW = "new"


DEFAULT_GRADING = {
    ProgressStatus.PUBLISHED: 1.0,
    ProgressStatus.TRIED: 0.0,
    ProgressStatus.OPENED: 0.0,
    ProgressStatus.NEW: 0.0,
}

DEFAULT_GRADING_BOOL = {
    ProgressStatus.PUBLISHED: True,
    ProgressStatus.TRIED: False,
    ProgressStatus.OPENED: False,
    ProgressStatus.NEW: False,
}


class Group(NamedTuple):
    slug: str
    name: str
    course: str
    description_text: str
    default_language: str
    created_at: date
    last_activity: Optional[datetime]
    has_leaderboard: bool
    visible_profiles: bool
    visible_member_solutions: bool
    visible_site_solutions: bool
    membership_admission: str
    is_auto_following_enabled: bool
    is_current_group: bool
    is_email_review_activated: bool
    is_group_default_filter: bool
    is_full_access: bool
    is_monthly_leaderboard_default: bool
    is_revisions_enabled: bool
    n_members: int

    JSON_PROPERTIES = {
        "is_member_profiles_visible_for_site": "visible_profiles",
        "is_member_solutions_visible_for_site": "visible_member_solutions",
        "is_site_solutions_visible_for_members": "visible_site_solutions",
        "members_count": "n_members",
        "is_leaderboard_enabled": "has_leaderboard",
    }

    @classmethod
    def from_json(cls, obj: dict) -> "Group":
        """
        Read group from JSON representation.
        """
        props = cls.JSON_PROPERTIES
        kwargs = {props.get(k, k): v for k, v in obj.items()}
        kwargs["created_at"] = date.fromisoformat(kwargs["created_at"])
        if kwargs["last_activity"]:
            kwargs["last_activity"] = datetime.fromisoformat(kwargs["last_activity"])
        return cls(**kwargs)

    def to_json(self):
        """
        Convert object to json data structure.
        """
        data = dict(zip(self._fields, self))
        data["created_at"] = self.created_at.isoformat()
        if self.last_activity:
            data["last_activity"] = self.last_activity.isoformat()
        return data


class Progress(NamedTuple):
    status: ProgressStatus
    username: str
    question: str
    solutions: List[str]
    opened_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    solved_at: Optional[datetime] = None

    @property
    def is_published(self):
        return self.status == ProgressStatus.PUBLISHED

    @classmethod
    def from_json(cls, json: dict, question=None):
        kwargs = json.copy()
        kwargs["status"] = ProgressStatus(kwargs["status"])
        if question is not None:
            kwargs["question"] = question

        if date := kwargs.pop("openedAt", None):
            kwargs["opened_at"] = datetime.fromisoformat(date)
        if date := kwargs.pop("startedAt", None):
            kwargs["opened_at"] = datetime.fromisoformat(date)
        if date := kwargs.pop("solvedAt", None):
            kwargs["solved_at"] = datetime.fromisoformat(date)

        return Progress(**kwargs)

    def meets_deadline(self, deadline: datetime) -> bool:
        """
        Return True if solution was given before the given deadline.
        """
        return self.is_published and self.solved_at <= deadline


class Groups(dict[str, Group]):
    """
    A mapping between group slugs and group objects.
    """

    def to_dataframe(self, columns: List[str] = None) -> pd.DataFrame:
        """
        Convert group to Dataframe.
        """
        if columns is None:
            columns = list(Group._fields)
        data = pd.DataFrame.from_records([*self.values()], columns=columns)
        data.index = [*self.keys()]
        return data


def groups(token: str) -> Groups:
    """
    Groups Details List.
    """
    url = f"{API_URL}/{GROUP_DETAILS}/?token={token}"
    data = requests.get(url).json()
    result = Groups()
    for obj in data["objects"]:
        result[obj["slug"]] = Group.from_json(obj)
    return result


def progress(
    slug: str,
    token: str,
    only_published: bool = False,
    deadline: Optional[datetime] = None,
) -> List[Progress]:
    """
    Groups Details List.
    """
    url = f"{API_URL}/{GROUP_PROGRESS}/?token={token}&slug={slug}"
    data = requests.get(url).json()

    if "objects" not in data:
        console.log(data, log_locals=True)
        raise RuntimeError(data.get("message", "Unknown error"))

    response = []
    for question in data["objects"]:
        question_slug = question["slug"]
        for elem in question["data"]:
            progress = Progress.from_json(elem, question_slug)

            if only_published and not progress.is_published:
                continue
            if deadline and not progress.meets_deadline(deadline):
                continue

            response.append(progress)
    return response


def questions(slug: str, token: str, **kwargs) -> Dict[str, Set[str]]:
    """
    Return a list of questions solved by each student.
    """

    responses = defaultdict(set)
    for item in progress(slug, token, only_published=True, **kwargs):
        responses[item.username].add(item.question)
    return dict(responses)


def question_dataframe(
    slug: str,
    token: str,
    grades: Union[dict, callable] = None,
    empty_grade=None,
    bool=False,
    **kwargs,
) -> "pd.DataFrame":
    """
    Return questions table as a dataframe.
    """
    responses = defaultdict(dict)

    if grades is None:
        grades = DEFAULT_GRADING_BOOL if bool else DEFAULT_GRADING
    grade_fn = grades.get if not callable(grades) else grades

    if empty_grade is None:
        empty_grade = grade_fn(ProgressStatus.NEW)

    for item in progress(slug, token, only_published=True, **kwargs):
        try:
            grade = responses[item.question][item.username]
        except KeyError:
            grade = empty_grade
        responses[item.question][item.username] = max(grade, grade_fn(item.status))

    import pandas as pd

    df = pd.DataFrame.from_records(responses).fillna(empty_grade)
    df.sort_index(axis=0, inplace=True, key=lambda idx: idx.str.lower())
    df.sort_index(axis=1, inplace=True, key=lambda idx: idx.str.lower())
    df.index.name = "checkio_id"
    return df
