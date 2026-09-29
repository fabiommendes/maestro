import abc
from pydantic import Field
from typing import (
    Annotated,
    Any,
    ClassVar,
    Literal,
    Optional,
    Union,
    TYPE_CHECKING,
)
from pathlib import Path
import fred


from .base import Model, Grade

if TYPE_CHECKING:
    from .log import GradeExam

TYPES = {}
register = lambda cls: TYPES.setdefault(cls.TAG, cls)
__all__ = ["HelperBase", "Helper", "RunHelper", "CheckIOHelper"]


class HelperBase(abc.ABC):
    """
    Abstract class for manual assessment helper function.
    """

    def payload(self, path: Path, job: "GradeExam") -> Any:
        """
        Read data
        """
        raise NotImplementedError

    def auto_grade(self, grade: Optional[Grade], payload: Any) -> Optional[Grade]:
        """
        Return a modified grade if auto-grading can be computed.
        """
        return grade

    def run(self, payload, student):
        """
        Run helper and show information on screen.
        """
        raise NotImplementedError


@register
class RunHelper(Model, HelperBase):
    TAG: ClassVar[str] = "run"
    kind: str
    tag: Literal["run"] = "run"

    @classmethod
    def parse_obj(cls, obj):
        if isinstance(obj, str):
            obj = {"kind": obj}
        if isinstance(obj, fred.Tag):
            if obj.tag != cls.TAG:
                raise ValueError(f"invalid tag: {obj.tag}")
            obj = {"kind": obj.value}
        return super().parse_obj(obj)

    def payload(self, path):
        return path

    def run(self, payload, student):
        print(payload)


@register
class CheckIOHelper(Model, HelperBase):
    TAG: ClassVar[str] = "check-io"
    tag: Literal["check-io"] = "check-io"
    type: str

    def payload(self, path: Path) -> Any:
        return (path, path.read_text())

    def run(self, payload, student):
        path, src = payload
        print(path)
        print(src)


Helper = Annotated[Union[RunHelper, CheckIOHelper], Field(discriminator="tag")]
