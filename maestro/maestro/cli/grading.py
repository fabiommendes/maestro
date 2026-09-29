from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Footer, Header

from .app import BaseScreen


class Grading(BaseScreen):
    """
    Screen for managing grades and grading tasks.
    """

    TITLE = "Grading"
    SUB_TITLE = "Grade activities"

    def compose(self) -> ComposeResult:
        yield Header()
        yield Footer()


if __name__ == "__main__":
    from textual.app import App

    class TestApp(App):
        def get_default_screen(self) -> Screen:
            return Grading()

    app = TestApp()
    app.run()
