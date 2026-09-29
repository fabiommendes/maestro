"""
CLI utilities.
"""

from typing import Optional, Dict, Any, TYPE_CHECKING
import re

from sidekick.proxy import import_later, deferred

if TYPE_CHECKING:
    from rich.console import Console
else:
    Console = import_later("rich.console:Console")

__all__ = ["out", "err", "yn", "ask", "try_ask"]

out: "Console" = deferred(Console)  # type: ignore
err: "Console" = deferred(Console, stderr=True)  # type: ignore
CHOICES = re.compile(r"\[([a-zA-Z|/\s+]+)\]$")
CHOICES_SPLIT = re.compile(r"[|/\s*]")


def yn(x: bool, plain=False) -> str:
    """
    Convert boolean to "yes" or "no".
    """
    if plain:
        return "yes" if x else "no"
    return "[green]yes[/]" if x else "[red]no[/]"


def ask(prompt: str, console=out) -> bool:
    """
    Ask a yes/no question.
    """
    res = try_ask(prompt, console)
    if res is None:
        return ask(prompt, console)
    return res


def try_ask(prompt: str, console=out) -> Optional[bool]:
    """
    Ask a yes/no/cancel question.
    """
    out = input(prompt).lower()
    if out in ("y", "yes"):
        return True
    elif out in ("n", "no"):
        return False
    elif not out:
        return None
    else:
        return try_ask(prompt, console)


def choices(prompt: str, choices: Dict[str, Any], console=out, key=lambda x: x) -> Any:
    """
    Select a choice from options.
    """
    if not hasattr(choices, "items"):
        choices = dict((k, k) for k in choices)
    choices, msgs = {key(k): v for k, v in choices.items()}, list(choices.keys())
    values = list(choices.values())

    out.print(prompt)
    for i, msg in enumerate(msgs, 1):
        out.print(f"[b green]{i}.[/] {msg}")

    while True:
        msg = out.input("Select an option: ")
        try:
            return choices[key(msg)]
        except KeyError:
            pass
        try:
            return values[int(msg) - 1]
        except (IndexError, ValueError):
            pass
        out.print("[warn]Invalid option[/]")
