from dataclasses import dataclass, field

import pandas as pd
from textual.app import ComposeResult
from textual.events import Key
from textual.reactive import reactive
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Label

from .app import BaseScreen


@dataclass
class Data:
    data: pd.DataFrame = field(default_factory=pd.DataFrame)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Data):
            return self.data is other.data
        return False

    def copy(self) -> pd.DataFrame:
        return self.data.copy()


class ListStudents(BaseScreen):
    """
    Screen for managing students.
    """

    TITLE = "Students"
    SUB_TITLE = "Manage enrolled students"
    BINDINGS = [
        ("escape", "escape", "Back"),
    ]

    filter_string = reactive("", repaint=True)
    students_data = reactive[Data](Data, repaint=True)

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("Filter:", id="filter_label")
        yield DataTable(id="students", fixed_columns=1, zebra_stripes=True)
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#students", DataTable)
        self.students_data = Data(self.classroom.students_dataframe())
        self._populate_table(table)
        self.app.bind("a", "", show=False)
        self.app.bind("s", "", show=False)
        self.app.bind("g", "", show=False)

    def on_key(self, event: Key) -> None:
        repaint = False
        if event.key == "backspace" and self.filter_string:
            self.filter_string = self.filter_string[:-1]
            repaint = True
        if event.character:
            self.filter_string += event.character
            repaint = True
            event.stop()
        if event.key == "escape" and self.filter_string:
            self.filter_string = ""
            repaint = True

        label = self.query_one("#filter_label", Label)
        label.update(f"Filter: [green bold]{self.filter_string}[/]")

        if repaint:
            table = self.query_one("#students", DataTable)
            self._populate_table(table)

    def action_escape(self) -> None:
        if self.filter_string:
            self.filter_string = ""
        else:
            self.app.switch_mode("home")

    def _populate_table(self, table: DataTable) -> None:
        table.clear(columns=True)
        df = self._filtered_students_data()

        hidden_columns = self.classroom.students_hidden_columns()
        hidden_columns.append("id")
        columns = ["id"]

        # Create columns
        table.add_column("id")
        for col in df.columns:
            if col in hidden_columns:
                continue
            columns.append(col)
            table.add_column(col, width=20)

        # Populate
        df["id"] = df.index
        for _, row in df.iterrows():
            table.add_row(*(row[col] for col in columns))

    def _filtered_students_data(self) -> pd.DataFrame:
        df = self.students_data.copy()
        df_data = df.astype(str)

        substring = self.filter_string
        if not substring:
            return df

        def filter_row(row: pd.Series) -> bool:
            contains = row.str.contains(substring, case=False)
            return any(contains.values)

        mask = df_data.apply(filter_row, axis=1)
        return df[mask]


if __name__ == "__main__":
    from textual.app import App

    class TestApp(App):
        def get_default_screen(self) -> Screen:
            return ListStudents()

    app = TestApp()
    app.run()
