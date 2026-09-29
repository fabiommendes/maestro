import abc
from dataclasses import dataclass
from functools import cached_property
from hashlib import md5
from pathlib import Path
from typing import Callable, TypeVar, TYPE_CHECKING

from teach.core import Model

from .base import StudentId
from .student import Student

if TYPE_CHECKING:
    from .question import Question

HashTransform = Callable[[Path, bytes], bytes]
Sub = TypeVar("Sub", bound="Submission")


def HASH_ID(_, data: bytes) -> bytes:
    return data


class Submission(Model, abc.ABC):
    """
    Base class for all submissions.

    It represents a single submission of a student to a particular problem.
    """

    student: Student
    question: "Question"

    @property
    def student_id(self) -> StudentId:
        return self.student.id

    @classmethod
    def parse_obj(
        cls: type[Sub], obj: object, student: Student, question: "Question"
    ) -> Sub:
        """
        Parse the given object and convert it to a submission.
        """
        raise NotImplementedError

    def hash(self, transform: HashTransform = HASH_ID) -> bytes:
        """
        Return the hash of the submission using the given hasher.
        """
        raise NotImplementedError


@dataclass
class TextSubmission(Submission):
    """
    Main content of submission is a string of text.
    """

    student: Student
    question: "Question"
    text: str

    @property
    def bytes(self) -> bytes:
        return self.text.encode("utf-8")

    @classmethod
    def parse_obj(cls, obj, student: Student, question: "Question") -> "Submission":
        if isinstance(obj, Path):
            obj = obj.read_text()
        if isinstance(obj, bytes):
            obj = obj.decode("utf-8")
        return cls(student, question, str(obj))

    def hash(self, transform: HashTransform = HASH_ID):
        return md5(transform(Path("input"), self.bytes)).digest()


@dataclass
class BytesSubmission(Submission):
    """
    Main content of submission as bytes.
    """

    student: Student
    question: "Question"
    bytes: bytes

    @property
    def text(self) -> str:
        return self.bytes.decode("utf-8")

    @classmethod
    def parse_obj(cls, obj, student: Student, question: "Question") -> "Submission":
        if isinstance(obj, Path):
            obj = obj.read_bytes()
        elif isinstance(obj, str):
            obj = obj.encode("utf-8")
        elif isinstance(obj, bytes):
            pass
        else:
            raise ValueError(f"invalid type: {type(obj)}")
        return cls(student, question, obj)

    hash = TextSubmission.hash  # type: ignore


@dataclass
class PathSubmission(Submission):
    """
    Main content of submission is stored in a path in the filesystem.

    If path is a file, it's contente defines the submission's data content.
    """

    student: Student
    question: "Question"
    path: Path

    def text(self) -> str:
        return self.bytes.decode("utf-8")

    @cached_property
    def bytes(self) -> bytes:
        if self.path.is_file():
            return self.path.read_bytes()
        raise ValueError(f"{self.path} is a directory")

    def hash(self, transform=HASH_ID):
        hash = md5()
        if self.path.is_file():
            hash.update(transform(self.path, self.path.read_bytes()))
        else:
            for p in self.path.rglob("*"):
                if p.is_file():
                    hash.update(transform(p, p.read_bytes()))
        return hash.digest()
