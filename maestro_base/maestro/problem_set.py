from __future__ import annotations

from abc import ABC
from collections import defaultdict
from pathlib import Path
from typing import cast, Callable, TypeVar, Optional, Dict, Iterator, Tuple, Iterable
import pandas as pd
import json
from unidecode import unidecode

#
# TypeAliases
#
from aulas import json

K = TypeVar("K")
K_ = TypeVar("K_")
V = TypeVar("V")
V_ = TypeVar("V_")
QuestionId = str
StudentId = str
Submission = dict
Info = dict
QuestionDb = Dict[QuestionId, Dict[StudentId, Submission]]
DbItem = Tuple[QuestionId, StudentId, Submission]


def iter_db(db: QuestionDb) -> Iterator[DbItem]:
    """
    Iterate over contents of a QuestionDb.
    """
    for question_id, submissions in db.items():
        for student_id, submission in submissions.items():
            yield question_id, student_id, submission


def save_submission(
    path: Path, question_id: QuestionId, student_id: StudentId, submission: Submission
):
    """
    Save item in the given path in the filesystem.
    """
    loc = path / question_id / student_id
    (loc / "data").mkdir(exist_ok=True, parents=True)
    info = submission.copy()
    answer = info.pop("answer")
    (loc / "info.json").write_text(json.dumps(info))
    (loc / "answer.data").write_text(answer)


def get_info(path: Path, question_id: QuestionId, student_id: StudentId) -> Info:
    """
    Get info about given question.
    """
    with open(path / question_id / student_id / "info.json") as fd:
        return json.load(fd)


def get_answer(
    path: Path, question_id: QuestionId, student_id: StudentId
) -> Submission:
    """
    Get submission about given question in the .
    """

    with open(path / question_id / student_id / "answer.data") as fd:
        return fd.read()


def update_info(
    path: Path, question_id: QuestionId, student_id: StudentId, func=None, /, **kwargs
) -> Info:
    """
    Apply function to the info dictionary of the given submission.
    """
    path = path / question_id / student_id / "info.json"
    with open(path) as fd:
        info = json.load(fd)

    if func is None:
        info.update(kwargs)
    else:
        func(info, **kwargs)

    with open(path, "w") as fd:
        json.dump(info, fd)

    return info


def get_submission(
    path: Path, question_id: QuestionId, student_id: StudentId
) -> Submission:
    """
    Get submission about given question in the .
    """
    info = get_info(path, question_id, student_id)
    answer = get_answer(path, question_id, student_id)
    info["answer"] = answer
    return answer


class Extractor(ABC):
    """
    Basic interface for extracting questions from a source and persisting it to
    the filesystem.
    """

    def extract_submissions(self) -> Iterator[DbItem]:
        """
        Extract question db.
        """
        raise NotImplementedError

    def collect_submissions(self) -> QuestionDb:
        """
        Iterate over each extracted submission.
        """
        db = defaultdict(dict)
        for question_id, student_id, submission in self.extract_submissions():
            db[question_id][student_id] = submission
        return dict(db)

    def save_db(self, db: QuestionDb, path: Path):
        """
        Save the contents of the question database in dictionary
        """
        return self.save_items(iter_db(db), path)

    def save_items(self, items: Iterable[DbItem], path: Path):
        """
        Save the contents of the question database in dictionary without
        loading the whole submission set to memory.
        """
        for item in items:
            save_submission(path, *item)


class DataFrameExtractor(Extractor):
    """
    Extract solutions from a dataframe and create a the <submissions> tree
    with student submissions.

    Args:
        id_input_clean_substrings:
            A set of substrings that are removed from raw input. This is useful
            to clean input data removing unnecessary punctuation, comments, etc.
            Substrings are replaced by a single whitespace.
        id_input_replacements:
            A mapping from input string -> cleaned string that makes arbitrary
            transformations in the input data. Can be used as a last resort
            replacement for input data that cannot be processed by this class.
        id_replacement:
            A map that converts all question_id in keys to their corresponding
            values. Useful to accept synonyms. This mapping replaces the whole
            string and not sub-strings and is preferrable to id_sub_replacements
            when both are applicable.
        id_sub_replacements:
            A map from substring to replacement for arbitrary substring
            replacements done in processing each question_id.
    """

    ID_INPUT_CLEAN_SUBSTRINGS = frozenset()
    ID_INPUT_REPLACEMENTS = {}
    ID_REPLACEMENTS = {}
    ID_SUB_REPLACEMENTS = {}
    ID_SKIP = set

    @classmethod
    def read_options(cls, options) -> DataFrameExtractor:
        """
        Initialize extractor from options.
        """
        raise NotADirectoryError

    def __init__(self, data: pd.DataFrame, **kwargs):
        self.data = self.clean_dataframe(data)
        self.id_input_clean_substrings: set[str] = {
            *map(
                self.normalize_input,
                kwargs.pop("id_input_clean_substrings", self.ID_INPUT_CLEAN_SUBSTRINGS),
            )
        }
        self.id_input_replacements: dict[str, str] = map_keys(
            self.normalize_input,
            kwargs.pop("id_input_replacements", self.ID_INPUT_REPLACEMENTS),
        )
        self.id_replacements: dict[str, str] = map_keys(
            self.normalize_input, kwargs.pop("id_replacements", self.ID_REPLACEMENTS)
        )
        self.id_sub_replacements: dict[str, str] = map_keys(
            self.normalize_input,
            kwargs.pop("id_sub_replacements", self.ID_REPLACEMENTS),
        )
        self.skip_question_ids = set()
        self.valid_question_ids = None

    def extract_submissions(self) -> Iterator[DbItem]:
        for _, row in self.data.sort_values("timestamp").iterrows():
            student_id = row["student_id"].replace("/", "")
            submission = {
                "answer": row["answer"],
                "obs": None if (obs := row["obs"]) else obs,
                "timestamp": row["timestamp"],
            }
            for question_id in self.question_ids(row["question_id"]):
                yield question_id, student_id, submission.copy()

    def is_valid_student_id(self, id: str) -> bool:
        """
        Validates student Id.
        """
        return True

    def clean_dataframe(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Clean data frame before storing it in the self.data attribute.
        """
        return data

    def normalize_question_id(self, st: str) -> str:
        """
        Normalize question id strings. Default implementation is the identity function.
        """
        return st

    def normalize_input(self, st: str) -> str:
        """
        Normalize input strings before passing to parse_question_id_input.

        This function is also used to construct the the input_strip_table.
        string_replacement_table attributes
        """
        return unidecode(st.replace("/", " ").replace("_", "-").lower())

    def question_ids(self, st: str) -> set[str]:
        """
        Read the raw question_id string and return a set of unique strings
        corresponding to the question_ids that relevant to the input field.
        """

        st = self.id_input_replacements.get(st, st)
        for sub in self.id_input_clean_substrings:
            st = st.replace(sub, "")
        try:
            return {
                question_id
                for tk in st.split()
                if (question_id := self.parse_question_id(tk)) is not None
            }
        except ValueError as ex:
            raise ValueError(st, ex)

    def parse_question_id(self, qid: str) -> Optional[str]:
        """
        Parse a single question_id string token.
        """
        if not qid:
            return None
        if qid[0].isdigit():
            return self.parse_question_id(qid[1:])
        if qid.endswith(".py"):
            qid = qid[:-3]
        qid = qid.strip(",.;-()[]{}'\"")
        qid = qid.removeprefix("test").strip("-")

        for part, subs in self.id_sub_replacements.items():
            qid = qid.replace(part, subs)

        if not qid:
            return None
        qid = self.normalize_question_id(qid)
        if qid in self.skip_question_ids:
            return None
        qid = self.id_replacements.get(qid, qid)
        if self.valid_question_ids is not None and qid not in self.valid_question_ids:
            raise ValueError(qid)

        return qid


class SubmissionGrader:
    """
    Submission
    """


class ProblemSetDb:
    ...


def map_keys(fn: Callable[[K], K_], dic: dict[K, V]) -> dict[K_, V]:
    """
    Map function to dict keys.
    """
    return {fn(k): v for k, v in dict.items()}


def map_values(fn: Callable[[V], V_], dic: dict[K, V]) -> dict[K, V_]:
    """
    Map function to dict values.
    """
    return {k: fn(v) for k, v in dict.items()}
