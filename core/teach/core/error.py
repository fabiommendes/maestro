import weakref
import enum
from typing import Any, ClassVar, Iterator, Optional, Protocol
from dataclasses import dataclass, field

from .typechecker import TypeHint, typecheck
from .normalizer import normalize
from .describable import Describable, Details

Location = str | int | None


class Error(Describable, Protocol):
    """
    Errors represent some sort of failure localized to an specific
    reference object (usually an instance) or object category (usually a type).

    Errors can displayed and linked in chains to provide cause and effect information.

    Attrs:
        severity:
            Severity level of the error.
        origin:
            The object that originated the error.
        location:
            A string or index pointing to where in the reference object the error occured.
            If None, the error is not localized in any sub-part of the object.
        cause:
            The error that caused this error, if any.

    """

    # Attributes
    goal: Any | None
    origin: "Origin"
    location: Location
    cause: Optional["Error"]
    severity: "ErrorSeverity"

    # Class constants
    key: ClassVar[str] = "error"
    SEVERITY_LOW: ClassVar["ErrorSeverity"] = None  # type: ignore
    SEVERITY_SEVERE: ClassVar["ErrorSeverity"] = None  # type: ignore
    SEVERITY_CRITICAL: ClassVar["ErrorSeverity"] = None  # type: ignore

    def __str__(self) -> str:
        return self.description()

    def __iter_details__(self) -> Details:
        yield ("line", self.description().splitlines())

    def goal_description(self) -> str:
        """
        A human-friendly rendering of the goal as a string.
        """
        return str(self.goal)

    def cause_chain(self) -> Iterator["Error"]:
        """
        Iterate over the chain of causes.
        """
        error = self
        while error is not None:
            yield error
            error = error.cause  # type: ignore

    def root_cause(self) -> "Error":
        """
        Return the root cause of error.
        """
        error = self
        for error in self.cause_chain():
            pass
        return error


# =============================================================================
# Helper types
# =============================================================================
class ErrorSeverity(enum.IntEnum):
    """
    Severity level for an error

    This describes the severity of the error to some
    """

    #: Mild problem or warning that can be recovered with minor or no issues
    #: during runtime. Thoses errors are usually triggered only in some sort
    #: of "strict" or "debug" mode.
    LOW = 0

    #: Error that can recovered with major issues. Perhaps they be ommited when
    #: executing in some sort of "lenient" mode.
    SEVERE = 1

    #: Regular error that completely prevents the goal from being achieved.
    CRITICAL = 2


Error.SEVERITY_LOW = ErrorSeverity.LOW
Error.SEVERITY_SEVERE = ErrorSeverity.SEVERE
Error.SEVERITY_CRITICAL = ErrorSeverity.CRITICAL


class Origin:
    """
    Value that can be in two states: Object or Category.
    """

    CATEGORIC_TYPES = (
        *(str, int, float, bool, complex, bytes, type(None)),
        *TypeHint.__args__,  # type: ignore
    )

    @property
    def value(self) -> Any:
        value = self._value
        if isinstance(value, weakref.ref):
            return value()
        return value

    @property
    def is_object(self) -> bool:
        return self._is_object

    @property
    def is_category(self) -> bool:
        return not self._is_object

    @classmethod
    def Object(cls, value: Any, ref=False) -> "Origin":
        "Construct an object, use ref=True to store a weak reference."
        if ref:
            value = weakref.ref(value)
        return cls(value, True)

    @classmethod
    def Category(cls, value: Any, ref=False) -> "Origin":
        "Construct a category"
        return cls(value, False)

    @classmethod
    def new(cls, value: Any) -> "Origin":
        """
        Create a new Source and select the state using the following rules.

        * Types and type-like objects are considered categories.
        * Atomic python types (int, float, str, bool, none, etc) are considered categories.
        * Other objects are considered objects.
        """
        if isinstance(value, cls.CATEGORIC_TYPES):
            return cls.Category(value)
        return cls.Object(value)

    def __init__(self, value: Any, is_object: bool) -> None:
        self._value = value
        self._is_object = is_object

    def __repr__(self) -> str:
        cons = "Object" if self._is_object else "Category"
        return f"Source.{cons}({self._value!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Origin):
            return self._value == other._value and self._is_object == other._is_object
        return NotImplemented

    def __hash__(self) -> int:
        return hash((self._value, self._is_object, type(self)))

    def category(self) -> Any:
        "Return the category for the origin value."
        if self._is_object:
            return type(self._value)
        return self._value


ORIGIN_NOT_GIVEN = Origin.Category(None)

# =============================================================================
# Error sub-classes
# =============================================================================
class ExceptionalError(Exception):
    """
    An error that can be used as an exception.
    """

    @property
    def args(self) -> tuple[Any, ...]:  # type: ignore
        fields = getattr(self, "__dataclass_fields__", None)
        if fields is None:
            fields = ("goal", "origin", "location", "cause", "severity")
        return tuple(getattr(self, f) for f in fields)


@dataclass(frozen=True)
class TimeoutError(ExceptionalError, Error):
    """
    Produced when execution of some task timed-out.

    The goal is the desired time frame.
    """

    goal: float
    origin: "Origin" = ORIGIN_NOT_GIVEN
    location: Location = None
    cause: Optional["Error"] = None
    severity: "ErrorSeverity" = Error.SEVERITY_SEVERE
    key = "timeout-error"

    @property
    def timeout(self) -> float:
        return self.goal

    def description(self) -> str:
        return f"execution cancelled after timeout ({self.timeout} sec)"

    def goal_description(self) -> str:
        return f"execute under {self.timeout}"


@dataclass(frozen=True)
class RuntimeError(ExceptionalError, Error):
    error_code: int = 1
    origin: "Origin" = ORIGIN_NOT_GIVEN
    location: Location = None
    cause: Optional["Error"] = None
    severity: "ErrorSeverity" = Error.SEVERITY_SEVERE
    goal = "complete execution without errors"
    key = "runtime-error"

    def __post_init__(self):
        if self.error_code == 0:
            raise ValueError("error_code cannot be 0")

    def description(self) -> str:
        return f"execution finished with non-zero error code {self.error_code}"


@dataclass(frozen=True)
class ConfigurationError(ExceptionalError, Error):
    """
    Produced when a configuration of some system parameter or
    attribute is invalid.
    """

    location: Location = None
    origin: "Origin" = ORIGIN_NOT_GIVEN
    msg: str = "invalid configuration"
    cause: Optional["Error"] = None
    severity: "ErrorSeverity" = Error.SEVERITY_SEVERE
    goal = None
    key = "configuration-error"

    def description(self) -> str:
        return f"{self.location} has an invalid value!"

    def goal_description(self) -> str:
        return "A properly configured system"


@dataclass(frozen=True)
class CompilationError(ExceptionalError, Error):
    """
    Produced when a compilation of some code fails.
    """

    msg: str
    lang: str = "code"
    origin: "Origin" = ORIGIN_NOT_GIVEN
    location: Location = None
    cause: Optional["Error"] = None
    severity: "ErrorSeverity" = Error.SEVERITY_SEVERE
    goal = None
    key = "compilation-error"

    def description(self) -> str:
        msg = self.msg.rstrip().rsplit("\n", 1)[-1]
        return f'compilation error: "{msg}"'

    def goal_description(self) -> str:
        return "Compilation"


@dataclass(frozen=True)
class WrongTypeError(ExceptionalError, Error):
    """
    Error raised when a value is of the wrong type.

    Severity usually follows the following semantics:
    - LOW: Value is a sub-type, but we are being strict about exact types.
    - SEVERE: Value is of a wrong type that can be coerced to the correct
      type without loss of information.
    - CRITICAL: Value is of the wrong type.
    """

    got: TypeHint
    expect: TypeHint
    location: str | int | None = None
    severity: ErrorSeverity = ErrorSeverity.CRITICAL
    cause: Optional["Error"] = None
    origin: Origin = field(default=ORIGIN_NOT_GIVEN, kw_only=True, repr=False)

    # Class constants
    goal = "typecheck"

    @classmethod
    def typecheck(
        cls,
        value: Any,
        expect: TypeHint,
        strict: bool = False,
        cause: Error | None = None,
    ) -> Optional["WrongTypeError"]:
        """
        Type checks value using class.
        """
        typ = type(value)

        if typ is expect:
            return None
        elif strict:
            if typecheck(value, expect):
                return cls(typ, expect, severity=ErrorSeverity.LOW, cause=cause)
            else:
                return cls(typ, expect, severity=ErrorSeverity.CRITICAL, cause=cause)
        else:
            if typecheck(value, expect):
                return None
            try:
                normalize(value, expect)
            except TypeError:
                return cls(typ, expect, severity=ErrorSeverity.CRITICAL, cause=cause)
            else:
                return cls(typ, expect, severity=ErrorSeverity.SEVERE, cause=cause)

    def __post_init__(self) -> None:
        if self.origin is ORIGIN_NOT_GIVEN:
            object.__setattr__(self, "origin", self.got)

    def description(self) -> str:
        return f"got {self.got}, expected {self.expect}"
