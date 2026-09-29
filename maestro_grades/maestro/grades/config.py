from logging import getLogger

from rich.console import Console
from rich.theme import Theme
from teach.core import Events

from .models import StudentId

log = getLogger("maestro.grades")
theme = Theme({"error": "red", "info": "green", "warn": "yellow"})
console = Console(theme=theme)
ctx = Events(fallback=console.print)


class ImplementationError(Exception):
    """
    Raised when code implementation is invalid.
    """


#
# Register event handlers
#
@ctx.register("student.missing", cls=StudentId)
def student_missing(ref: StudentId, _seen: set = set()):
    if ref not in _seen:
        _seen.add(ref)
        console.print(f"  [warn b]WARNING[/]: Student {ref} is missing.")
