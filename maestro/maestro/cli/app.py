from pathlib import Path
from typing import Any

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Label

from ..classroom import Classroom
from ..utils import camel_case_to_snake_case

NOT_GIVEN = NotImplemented


class MaestroApp(App):
    """
    Manage classrooms, students activities and grades.
    """

    MODES = {
        "activities": "list-activities",
        "students": "list-students",
        "grading": "grading",
        "home": "list-activities",
    }
    BINDINGS = [
        Binding(
            "a",
            "app.switch_mode('activities')",
            "Activities",
            tooltip="Manage activities",
        ),
        Binding(
            "s",
            "app.switch_mode('students')",
            "Students",
            tooltip="Manage students",
        ),
        Binding(
            "g",
            "app.switch_mode('grading')",
            "Grading",
            tooltip="Grade activities",
        ),
    ]
    TITLE = "Maestro"
    SUB_TITLE = "Automate students activities and grades"

    def __init__(self, classroom: Classroom, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.classroom = classroom

    def compose(self) -> ComposeResult:
        yield Header()

        if not self.classroom.exists():
            yield from self._compose_no_config()
        else:
            self.switch_mode("home")

        yield Footer()

    def _compose_no_config(self) -> ComposeResult:
        path = self.classroom.path.relative_to(Path.cwd())
        yield Label(f"Configuration file not found at {path}.")
        yield Label("Create new file at current dir?")
        yield Button("Yes", id="create_config", variant="primary")
        yield Button("No", id="exit_button")

    def _compose_config(self) -> ComposeResult:
        yield Label("Welcome to Maestro! Use the bindings to navigate.")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        match event.button.id:
            case "create_config":
                self.classroom.save()
            case "exit_button":
                self.exit()

    def action_toggle_dark(self) -> None:
        self.theme = (
            "textual-dark" if self.theme == "textual-light" else "textual-light"
        )

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        """
        Disable switching to a mode we are already on.
        """
        if (
            action == "switch_mode"
            and parameters
            and self.current_mode == parameters[0]
        ):
            return None
        return True

    # TODO: switch to new notification API
    def notify_cb(self, notification: Any) -> None:
        """
        Default notification callback for the app.
        """
        notification(self)


class BaseScreen(Screen):
    """
    Base screen for Maestro application.
    """

    NAME: str
    app: MaestroApp

    @property
    def classroom(self) -> Classroom:
        return self.app.classroom

    @classroom.setter
    def classroom(self, value: Classroom):
        self.app.classroom = value

    @classmethod
    def __init_subclass__(cls, **kwargs):
        if hasattr(cls, "SCREEN_NAME"):
            name = cls.NAME
        else:
            name = camel_case_to_snake_case(cls.__name__, sep="-")
            cls.NAME = name
        MaestroApp.SCREENS[name] = cls
        super().__init_subclass__(**kwargs)
