from __future__ import annotations

import enum
from pathlib import Path
from typing import Iterable, Optional, assert_never, cast

from textual.app import ComposeResult
from textual.binding import Binding
from textual.reactive import reactive
from textual.screen import Screen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    OptionList,
    Pretty,
    TextArea,
)

from ..activity import BaseActivity
from ..utils import slugify
from ..widgets.pandas_data_table import PandasDataTable
from .app import BaseScreen

GH_CLASSROOM_TEMPLATE = """type: github-classroom
name: {name} 
url: 
path: {path}
"""

SPREADSHET_TEMPLATE = """type: spreadsheet
name: {name}
url:
path: {path}
"""

TEXT_FILE_TEMPLATE = """type: spreadsheet
name: {name}
url:
path: {path}
"""


class ListActivities(BaseScreen):
    """
    List activities in the classroom.

    This screen allows users to view, create, and manage activities and is the
    entry point for activity management in the application.
    """

    BINDINGS = [
        Binding(
            "n",
            "app.switch_mode('new-activity')",
            "New",
            tooltip="Create a new activity",
        ),
        *(Binding(str(n), action=f"push_index('{n}')", show=False) for n in range(10)),
    ]
    TITLE = "Activities"
    SUB_TITLE = "Manage student activities and grades"

    def compose(self) -> ComposeResult:
        yield Header()

        if self.classroom.has_activities():
            data = self.classroom.assignemnts_table()
            data = data.reset_index()
            data.index += 1
            data.index.name = "#"
            yield PandasDataTable(data=data, id="activities")
        else:
            yield Label("No activities found. Create a new one.")

        yield Footer()

    def on_data_table_cell_selected(self, event: DataTable.CellSelected) -> None:
        df = cast(PandasDataTable, event.data_table).data
        key = int(event.cell_key.row_key.value or 0)
        slug = df["id"][key]
        screen = ViewActivity(self.classroom.get_activity(slug))
        self.app.push_screen(screen)


class ViewActivity(BaseScreen):
    """
    Screen for viewing an activity.
    """

    NAME = "view-activity"
    TITLE = "Activity"
    BINDINGS = [
        ("escape", "app.pop_screen()", "Back"),
        ("r", "run_pipeline", "Run Pipeline"),
    ]

    def __init__(self, activity: BaseActivity, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.activity = activity

    def action_run_pipeline(self) -> None:
        """
        Run the activity pipeline.
        """
        self.activity.run()

    def compose(self) -> ComposeResult:
        yield Header()
        yield Pretty(self.activity)
        yield Footer()

    def on_mount(self):
        self.sub_title = self.activity.name


class CreateActivity(BaseScreen):
    """
    Screen for creating a new activity.
    """

    NAME = "create-activity"
    TITLE = "New Activity"

    type = reactive[Optional["Kind"]](None, recompose=True)
    slug = reactive[str | None](None, recompose=True)

    def compose(self) -> ComposeResult:
        yield Header()

        if self.type is None or self.slug is None:
            yield Label("Slug identifier")
            yield Input(id="slug")
            yield Label("Select the kind of activity to create:")
            yield OptionList(*Kind, id="kind")
        else:
            title = self.slug.replace("-", " ").title()
            base = self.classroom.activities_path / self.slug
            if base.exists():
                yield Label("Error: activity already exists!")
            else:
                base.mkdir(parents=True)
                source = self.type.template(name=title, path=base / "config.yaml")
                yield TextArea.code_editor(source, language="yaml", id="yaml")
                yield Button("Submit", id="finish")

        yield Footer()

    def on_assignement_created(self, _):
        self.app.switch_mode("activities")

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        if event.option_list.id == "kind":
            slug_field = cast(Input, self.get_child_by_id("slug"))
            self.slug = slug_field.value
            self.type = cast(Kind, event.option.prompt)

    def on_input_blurred(self, event: Input.Blurred) -> None:
        if event.input.id == "activity_name":
            slug = slugify(event.input.value)
            slug_field = cast(Input, self.get_child_by_id("activity_slug"))
            slug_field.value = slug
        elif event.input.id == "activity_slug":
            # Handle activity slug submission
            pass
        elif event.input.id == "gh_classroom_url":
            # Handle GH Classroom URL submission
            pass

    def on_button_pressed(self, event: Button.Pressed) -> None:
        match event.button.id:
            case "gh_create":
                name = self.query_one("#activity_name", Input).value
                slug = self.query_one("#activity_slug", Input).value
                url = self.query_one("#gh_classroom_url", Input).value
                raise NotImplementedError
            case "finish":
                assert self.slug is not None

                textarea = cast(TextArea, self.get_child_by_id("yaml"))
                source = textarea.text
                base = self.classroom.activities_path / self.slug
                path = base / "config.yaml"
                path.write_text(source, encoding="utf8")
                self.app.switch_mode("activities")


class Kind(enum.StrEnum):
    """
    Enum to represent the kind of activity.
    """

    REPOSITORY = "Repository"
    SPREADSHEET = "Spreadsheet"
    TEXT_FILE = "Text File"

    def template(self, name: str, path: Path):
        match self:
            case Kind.REPOSITORY:
                base = GH_CLASSROOM_TEMPLATE
            case Kind.SPREADSHEET:
                base = SPREADSHET_TEMPLATE
            case Kind.TEXT_FILE:
                base = TEXT_FILE_TEMPLATE
            case _:
                assert_never(self)
        return base.format(name=name, path=str(path))


def is_empty(iterator: Iterable) -> bool:
    """
    Check if an iterable is empty.
    """
    for _ in iterator:
        return False
    return True


if __name__ == "__main__":
    from textual.app import App

    class TestApp(App):
        def get_default_screen(self) -> Screen:
            return ListActivities()

    app = TestApp()
    app.run()
