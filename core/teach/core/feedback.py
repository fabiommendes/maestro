from typing import Any, Sequence, TypeVar, Optional, Protocol, TYPE_CHECKING
from dataclasses import dataclass

from rich import get_console

from .describable import Describable, Details
from .models import JSONMixin
from .error import Error

if TYPE_CHECKING:
    from rich.console import Console

T = TypeVar("T")
ConsoleT = Optional["Console"]


class Feedback(JSONMixin, Describable, Protocol):
    """
    Feedback for a check.
    """

    key: str
    is_success: bool = False
    is_error: bool = True
    is_error = property(lambda self: not self.is_success)  # type: ignore

    @classmethod
    def Success(cls, msg: str = "", **kwargs) -> "SimpleFeedback":
        """
        Returned when action was skipped.
        """
        return SimpleFeedback("success", msg, is_success=True, **kwargs)

    @classmethod
    def Fail(cls, key, msg: str = "", payload: Any = None) -> "SimpleFeedback":
        """
        Returned when action was skipped.
        """
        return SimpleFeedback(key, msg, is_success=False, payload=payload)

    @classmethod
    def Error(cls, error: Error) -> "SimpleFeedback":
        """
        Returned when action was skipped.
        """
        key = type(error).__name__
        msg = error.description()
        return SimpleFeedback(key, msg, is_success=False, payload=error)

    @classmethod
    def Skip(cls, msg: str = "", *, is_success: bool = False) -> "SimpleFeedback":
        """
        Returned when action was skipped.
        """
        return SimpleFeedback("skip", msg, is_success=is_success)

    @classmethod
    def Multi(cls, feedbacks: Sequence["Feedback"], key=None) -> "Feedback":
        """
        Combine multiple feedbacks into one.
        """
        success_fb = cls.Success()
        errors = []
        for fb in _flatten_feebacks(feedbacks):
            if fb.is_success:
                success_fb = fb
            else:
                errors.append(fb)
        if not errors:
            return success_fb
        elif len(errors) == 1:
            return errors[0]
        else:
            if key is not None:
                key = errors[0].key
            return MultipleFeedback(feedbacks=errors, flatten=True, key=key)

    def __str__(self) -> str:
        return self.description()

    def __iter_details__(self) -> Details:
        yield ("line", self.description().splitlines())

    def print_rich(
        self,
        console: ConsoleT = None,
        detail: bool = False,
        interactive: bool = False,
    ) -> None:
        """
        Print the feedback using rich's pretty print library.
        """
        if console is None:
            from rich import get_console

            console = get_console()

        color = "red" if self.is_error else "green"
        console.print(f"[b {color}]{self.key}:[/]", self.description())


@dataclass
class SimpleFeedback(Feedback):
    """
    A simple feedback with key, message and payload.
    """

    key: str
    msg: str = ""
    payload: Any = None
    is_success: bool = False

    def __iter_details__(self) -> Details:
        yield "line", self.description().splitlines()

    def description(self):
        return self.msg or self.key

    def print_rich(
        self,
        console: ConsoleT = None,
        detail: bool = False,
        interactive: bool = False,
    ):
        console = get_console() if console is None else console
        super().print_rich(console, detail, interactive)
        if self.payload is not None:
            from rich.panel import Panel

            console.print(Panel(self.payload))


@dataclass
class MultipleFeedback(Feedback):
    key: str
    feedbacks: list[Feedback]
    flatten: bool = False
    is_success: bool = False

    def description(self) -> str:
        n = len(self.feedbacks)
        return f"{n} errors found!"

    def __iter_details__(self) -> Details:
        for i, fb in enumerate(self.feedbacks, 1):
            yield "title", [str(i), fb.key]
            yield "sub-title", [fb.description()]
            yield from fb.__iter_details__()

    def print_rich(
        self,
        console: ConsoleT = None,
        detail: bool = False,
        interactive: bool = False,
    ):
        console = get_console() if console is None else console
        console.print(f"{len(self.feedbacks)} Errors found ([b red]{self.key}[/])")
        for i, feedback in enumerate(self.feedbacks):
            if i >= 2 and (
                interactive and not _do_continue(console) or not interactive
            ):
                remain = len(self.feedbacks) - i
                console.print(f"Omitting {remain} more messages...")
                break
            console.print(f"\n[b green]Message #{i + 1}[/]")
            feedback.print_rich(console, detail=detail, interactive=interactive)


def print_rich(feedback: Feedback, console=None, detail=False):
    if detail:
        msgs = feedback.__iter_details__()
    else:
        msgs = [("line", feedback.description().splitlines())]
    print_rich_messages(msgs, console=console)


def print_rich_messages(details: Details, console: ConsoleT = None):
    """
    Interpret detail messages using rich.
    """
    console = get_console() if console is None else console
    for key, lines in details:
        for line in lines:
            console.print(key, line)


#
# Auxiliary functions
#
def _flatten_feebacks(feebacks):
    for fb in feebacks:
        if isinstance(fb, MultipleFeedback) and fb.flatten:
            yield from fb.feedbacks
        else:
            yield fb


def _do_continue(console: "Console") -> bool:
    value = console.input("Continue? [Yn] ").lower() or "y"
    if value in ("y", "yes"):
        return True
    elif value in ("n", "no"):
        return False
    else:
        return _do_continue(console)
