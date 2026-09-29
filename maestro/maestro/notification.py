from __future__ import annotations

from contextlib import contextmanager
from functools import partial
from typing import Literal, Protocol

import rich
from textual.app import App
from textual.notifications import SeverityLevel

type Kind = Literal["info", "warn", "error", "debug"]

NOTIFY_FUNCTION: NotificationFunction
TEXTUAL_KIND: dict[Kind, SeverityLevel] = {
    "info": "information",
    "warn": "warning",
    "error": "error",
}


class NotificationFunction(Protocol):
    def __call__(
        self,
        message: str,
        title: str,
        kind: Kind,
        icon: str,
    ):
        raise NotImplementedError("Subclasses must implement this method.")


def notify(
    message: str | Exception,
    /,
    *,
    title: str = "",
    kind: Kind = "info",
    icon: str | None = None,
    id: str | None = None,
):
    """
    Send a notification with the given title and message.

    Use rich print to display the notification in the console.

    Args:
        message (str):
            A string message or rich renderable object.
        title (str):
            The title of the notification.
        icon (str, optional):
            An optional emoji icon.
    """
    if isinstance(message, Exception) and not title:
        message = str(message)
    elif isinstance(message, Exception):
        message = f"{message.__class__.__name__}: {message}"

    NOTIFY_FUNCTION(message, title, kind, icon or "")


def _notify_rich_print(message, title: str, kind: Kind, icon: str):
    if icon:
        title = f"{title} {icon}"
    if title:
        rich.print(f"[{kind}]{title}[/]")
    rich.print(message)


def _notify_textual_app(app: App, message, title: str, kind: Kind):
    if kind == "debug":
        return
    app.notify(message, title=title, severity=TEXTUAL_KIND[kind])


NOTIFY_FUNCTION = _notify_rich_print


def set_app(app: App):
    """
    Set the notification function to use the Textual app's notify method.

    Args:
        app (App): The Textual application instance.
    """
    global NOTIFY_FUNCTION
    NOTIFY_FUNCTION = partial(_notify_textual_app, app)


def print_notify():
    """
    Set the notification function to use rich print for notifications.
    This is useful for debugging or when not using a Textual app.
    """
    global NOTIFY_FUNCTION
    NOTIFY_FUNCTION = _notify_rich_print


@contextmanager
def app_notify(app: App):
    """
    Context manager to temporarily set the notification function for an app.

    Args:
        app (App):
            The Textual application instance.
    """
    global NOTIFY_FUNCTION
    original_notify = NOTIFY_FUNCTION
    try:
        set_app(app)
        yield
    finally:
        NOTIFY_FUNCTION = original_notify
