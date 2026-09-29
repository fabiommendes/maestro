import abc
from typing import Any, Iterable, Protocol

Role = str
Details = Iterable[tuple[Role, Iterable[str]]]


class Describable(Protocol):
    """
    Abstract interface for objects that can be displayed in short vs. long form.

    This is the base class for many objects that provide human-friendly feedback to user.
    """

    def description(self) -> str:
        """
        Render the object as a string in a human-friendly way.

        This is useful to display in error messages, tracebacks, etc.
        """
        ...

    def details(self) -> str:
        """
        Render details as string.

        This method is more flexible than description() and the implementation can be
        repurposed to render or print the object in many different formats.

        Sub-classes should usually implement __iter_details__, not this method.
        """
        return render_details(self.__iter_details__())

    def __iter_details__(self) -> Details:
        """
        Detailed rendering of error as a sequence of strings pairs.

        Each pair of (role, description) represents a part of the return message
        and a hint of how should it be displayed or interpreted.
        """
        ...


def render_description(obj: Any) -> str:
    """
    Render the object as a string in a human-friendly way

    Uses either obj.description() or str(obj).
    """
    try:
        fn = obj.description
    except AttributeError:
        return str(obj)
    else:
        return fn()


def render_details(obj: Any) -> str:
    """
    Render the object as a string in a human-friendly way.
    """
    return _render_details(_details(obj))


def print_details(obj: Any) -> None:
    """
    Print object's details in a human-friendly way.
    """
    for role, parts in _details(obj):
        match role:
            case "line":
                for p in parts:
                    print(p)
            case "hidden":
                continue
            case _:
                for p in parts:
                    print(p, end="")


def _details(obj: Any) -> Details:
    """
    Extract obj.__iter_details__() or generate a suitable
    value for non-supported values.
    """
    try:
        return obj.__iter_details__()
    except AttributeError:
        # TODO: maybe a more descriptive description?
        return [("line", [str(obj)])]


def _render_details(parts: Details) -> str:
    """
    Render a sequence of parts as a string using the default
    encoding for roles.

    >>> render_details([("line", ["bar", "baz"])])
    'bar\\nbaz'
    """

    def _details(seq) -> Iterable[str]:
        for role, parts in seq:
            match role:
                case "line":
                    yield from (f"{p}\n" for p in parts)
                case "hidden":
                    continue
                case _:
                    yield from parts

    return "".join(_details(parts))
