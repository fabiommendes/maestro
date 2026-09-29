from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from dataclasses import dataclass, field
from pathlib import Path
from types import NoneType
from typing import Callable, Iterable, NamedTuple, TypedDict, cast

import rich
from rich.markdown import Markdown
from rich.panel import Panel
from rich.syntax import Syntax
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.reactive import reactive
from textual.screen import Screen
from textual.widgets import Footer, Header, Label, ListItem, ListView

from .app import BaseScreen

__all__ = ["grades", "Item", "Grading", "SortGrading"]

WORD_WRAP_FORMATS = {"md", "markdown", "txt", "text", "rst"}

type Data[Id, Grade] = list[Item[Id, Grade]]


class Item[Id, Grade = float](TypedDict):
    grade: Grade | None
    items: list[Id]


class Grading[Id, Grade](NamedTuple):
    grades: dict[Id, Grade]
    finished: bool


@dataclass
class SortedItem[Id]:
    id: Id
    text: str
    _siblings: list[SortedItem[Id]] = field(default_factory=list, init=False)
    show_siblings: bool = field(default=False, init=False)

    def add_sibling(self, sibling: SortedItem[Id]) -> None:
        """
        Add a sibling item to the list of siblings.
        """
        self._siblings.append(sibling)

    def pop_sibling(self) -> SortedItem[Id]:
        """
        Remove and return the last sibling item, if present.
        """
        try:
            return self._siblings.pop()
        except IndexError:
            raise ValueError("No siblings to pop") from None

    def num_siblings(self) -> int:
        """
        Return the number of siblings.
        """
        return sum(1 for _ in self) - 1

    def __iter__(self):
        """
        Iterate over the siblings.
        """
        yield self
        for sibling in self._siblings:
            yield from sibling

    def has_siblings(self) -> bool:
        """
        Check if there are any siblings.
        """
        return bool(self._siblings)


class SortGrading[Id, Grade = None](BaseScreen):
    """
    Screen for managing grades and grading tasks.
    """

    type Item[T] = SortedItem[T] | None

    TITLE = "Sort Grading"
    SUB_TITLE = "Grade activities"
    BINDINGS = [
        Binding("w", "move_up", "Up"),
        Binding("s", "move_down", "Down"),
        Binding("a", "join", "Join"),
        Binding("d", "unjoin", "Unjoin"),
        Binding("e", "end", "To end"),
        Binding("space", "add_level"),
    ]

    _items: list[Item[Id]]

    @property
    def index(self) -> int:
        return self.query_one(ListView).index or 0

    @index.setter
    def index(self, idx: int):
        views = self.query_one(ListView)

        for i, child in enumerate(views.children):
            if isinstance(child, ListItem):
                child.highlighted = i == idx

    def __init__(
        self,
        responses: dict[Id, str],
        *args,
        save_file: Path | None = None,
        on_save: Callable[[Data[Id, Grade], Path | None], None] | None = None,
        on_load: Callable[[Path], Data[Id, Grade]] | None = None,
        syntax: str | None = None,
        show_id: bool = True,
        grade_levels: list[Grade] | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if not responses:
            raise ValueError("Grades dictionary cannot be empty")

        if on_load is None:

            def on_load(file: Path) -> Data[Id, Grade]:
                with file.open("r", encoding="utf-8") as fd:
                    return json.load(fd)

        if on_save is None:

            def on_save(data: Data[Id, Grade], path: Path | None) -> None:
                if path is None:
                    return

                with path.open("w", encoding="utf-8") as fd:
                    json.dump(data, fd, indent=2)

        self._on_load = on_load
        self._on_save = on_save
        self._syntax = syntax
        self._save_file = save_file
        self._show_id = show_id
        self._grade_levels = grade_levels or None
        self._items = [
            *items_from_responses(responses, on_load=on_load, save_file=save_file)
        ]

    def as_app(self) -> App:
        """
        Return the app instance.
        """
        this = self

        class SortGradingApp(App):
            BINDINGS = [
                ("q", "done", "Done"),
            ]
            grading = reactive[None | Grading[Id, Grade]](None)

            def get_default_screen(self) -> Screen:
                return this

            def action_done(self) -> None:
                this.save()
                self.grading = Grading(this.grades(), True)
                self.exit()

        return SortGradingApp()

    def run(self) -> Grading[Id, Grade]:
        """
        Run the app.
        """
        app = self.as_app()
        try:
            app.run()
        except SystemExit:
            pass

        grading = getattr(app, "grading", None)
        if grading is None:
            return Grading(self.grades(), False)
        return grading

    def grades(self) -> dict[Id, Grade]:
        """
        Return the grades as a dictionary.
        """
        return grades_from_levels(self._items, self._grade_levels)

    def save(self):
        """
        Save the current state to the specified file.
        """
        data = []
        for idx, item in enumerate(self._items):
            if item is None and self._grade_levels is None:
                continue
            elif item is None:
                item = {
                    "grade": self._grade_for_level(idx),
                    "items": [],
                }
            else:
                item = {
                    "grade": self._grade_for_level(idx),
                    "items": [s.id for s in item],
                }
            data.append(item)

        self._on_save(data, self._save_file)

    def _view(self, idx: int, item: Item[Id]) -> ListItem:
        text: str | Syntax | Markdown

        if item is None:
            grade = self._grade_for_level(idx)
            if grade is None:
                text = "[b red]Empty level[/]"
            else:
                text = f"[b red]Emtpy level[/] (grade {grade})"
            panel = Panel(
                text,
                border_style="#666666",
                style="dim",
            )
            return ListItem(Label(panel, expand=True))

        # render submission
        if self._syntax is None:
            text = item.text
        elif self._syntax in ("md", "markdown"):
            text = Markdown(item.text)
        else:
            text = Syntax(
                item.text,
                self._syntax,
                word_wrap=self._syntax in WORD_WRAP_FORMATS,
            )

        panel = Panel(
            text,
            title=self._title(idx, item),
            title_align="left",
            subtitle=f"by: [b green]{item.id}[/]" if self._show_id else None,
            subtitle_align="left",
            border_style=self._color_from_size(item.num_siblings()),
        )
        return ListItem(Label(panel, expand=True))

    def _title(self, idx: int, item: SortedItem[Id]) -> str:
        grade = self._grade_for_level(idx)
        n = item.num_siblings() + 1
        title = f"[white b]{n}[/] item"
        if n > 1:
            title += "s"
        if grade is not None:
            title += f" [yellow b](grade: {grade})[/]"
        return title

    def _grade_for_level(self, idx: int) -> Grade | None:
        if self._grade_levels is None:
            return None
        try:
            return self._grade_levels[idx]
        except IndexError:
            return self._grade_levels[-1]

    def _color_from_size(self, size: int) -> str:
        shade = ["6", "8", "a", "c", "e"]
        try:
            return "#" + shade[size] * 6
        except IndexError:
            return "white"

    def compose(self) -> ComposeResult:
        yield Header()
        yield ListView(*(self._view(i, item) for i, item in enumerate(self._items)))
        yield Footer()

    def update_title(self, idx: int) -> None:
        """
        Update the title of the item at the specified index.
        """
        item = self._items[idx]
        if item is None:
            return
        views = self.query_one(ListView)
        label = cast(Label, views.children[idx].children[0])
        cast(Panel, label.renderable).title = self._title(idx, item)
        label.refresh()

    async def _update_grades(self, from_index: int = 0):
        """
        Update the grades from a specific index.
        """
        if self._grade_levels is None:
            return

        for i, item in enumerate(self._items[from_index:], start=from_index):
            if i > len(self._grade_levels) - 1:
                return

            item = self._items[i]
            views = self.query_one(ListView)
            await views.insert(i, [self._view(i, item)])
            await views.remove_items([i + 1])

    async def insert(self, idx: int, items: Item[Id] | list[Item[Id]]) -> None:
        """
        Insert an item at the specified index.
        """
        if idx < 0 or idx > len(self._items):
            raise IndexError("Index out of bounds")
        if isinstance(items, (NoneType, SortedItem)):
            items = [items]

        for item in items:
            self._items.insert(idx, item)

        views = self.query_one(ListView)
        update = [self._view(i, item) for i, item in enumerate(items, start=idx)]
        await views.insert(idx, update)
        await self._update_grades(from_index=idx)

    async def append(self, item: Item[Id]) -> None:
        """
        Append an item to the end of the list.
        """
        views = self.query_one(ListView)
        views.append(self._view(len(views), item))
        self._items.append(item)

    async def pop_items(self, indices: list[int]) -> list[Item[Id]]:
        """
        Pop items from the list at the specified indices.
        """
        if not indices:
            return []
        if min(indices) < 0 or max(indices) >= len(self._items):
            raise IndexError("Index out of bounds")

        popped = [self._items[i] for i in indices]
        self._items = [item for i, item in enumerate(self._items) if i not in indices]
        views = self.query_one(ListView)
        await views.remove_items(indices)
        await self._update_grades(from_index=min(indices))
        return popped

    async def action_move_up(self) -> None:
        """
        Move the selected item up in the list.
        """
        if len(self._items) == 1:
            return

        views = self.query_one(ListView)
        idx = views.index or 0
        if idx == 0:
            return

        items = self._items
        b, a = items[idx], items[idx - 1] = items[idx - 1], items[idx]
        update = [self._view(idx - 1, a), self._view(idx, b)]
        await views.insert(idx - 1, update)
        await views.remove_items([idx + 1, idx + 2])
        views.index = idx - 1
        self.save()

    async def action_move_down(self) -> None:
        """
        Move the selected item down the list.
        """
        if len(self._items) == 1:
            return

        views = self.query_one(ListView)
        idx = views.index or 0
        items = self._items
        try:
            fst, snd = items[idx], items[idx + 1]
            items[idx + 1], items[idx] = fst, snd
        except IndexError:
            return

        update = [self._view(idx, snd), self._view(idx + 1, fst)]
        await views.insert(idx, update)
        await views.remove_items([idx + 2, idx + 3])
        views.index = idx + 1
        self.save()

    async def action_join(self) -> None:
        """
        Join the selected item with the next one.
        """
        idx = self.index
        try:
            item, next = self._items[idx], self._items[idx + 1]
        except IndexError:
            return

        if item is None:
            await self.pop_items([idx])
            self.index = idx
        elif next is None:
            await self.pop_items([idx + 1])
        else:
            item.add_sibling(next)
            self.update_title(idx)
            await self.pop_items([idx + 1])
        self.save()

    async def action_unjoin(self) -> None:
        """
        Join the selected item with the next one.
        """
        item = self._items[self.index]
        if item is None or not item.has_siblings():
            return

        idx = self.index
        next = item.pop_sibling()
        self.update_title(idx)
        await self.insert(idx + 1, next)
        self.save()

    async def action_add_level(self) -> None:
        """
        Add a new grade level.
        """
        await self.insert(self.index + 1, None)
        self.save()

    async def action_end(self) -> None:
        """
        Send selected item to end.
        """
        idx = self.index
        items = await self.pop_items([idx])
        await self.append(items[0])
        self.index = idx

    def action_debug_crash(self) -> None:
        with redirect_stdout(io.StringIO()) as fd:
            rich.print(self._items)
        raise RuntimeError(fd.getvalue())


def grades[Id, Grade = float](
    responses: dict[Id, str],
    grade_levels: list[Grade] | None = None,
    on_load: Callable[[Path], Data[Id, Grade]] | None = None,
    save_file: Path | None = None,
) -> dict[Id, Grade]:
    """
    Convert a dictionary of responses into a dictionary of grades.
    """

    if on_load is None:

        def on_load(file: Path) -> Data[Id, Grade]:
            with file.open("r", encoding="utf-8") as fd:
                return json.load(fd)

    items = list(items_from_responses(responses, save_file=save_file, on_load=on_load))
    data = grades_from_levels(items, grade_levels=grade_levels)
    return data


def grades_from_levels[Id, Grade = float](
    items: list[SortedItem[Id] | None], grade_levels: list[Grade] | None = None
) -> dict[Id, Grade]:
    """
    Return the grades as a dictionary.
    """
    grades: dict[Id, Grade] = {}

    if grade_levels is None:
        N = len(items)
        levels = [cast("Grade", n / (N - 1)) for n in reversed(range(N))]
    else:
        levels = grade_levels

    for item, grade in zip(items, levels):
        if item is None:
            continue
        for elem in item:
            grades[elem.id] = grade

    return grades


def items_from_responses[Id, Grade = float](
    responses: dict[Id, str],
    *,
    on_load: Callable[[Path], Data[Id, Grade]],
    save_file: Path | None = None,
) -> Iterable[SortedItem[Id] | None]:
    """
    Convert a dictionary of responses into a list of items.
    """
    responses = responses.copy()
    if save_file is not None and save_file.exists():
        data = on_load(save_file)
        for entry in data:
            ids = entry["items"]
            if not ids:
                yield None
                continue
            id, *rest = ids
            item = SortedItem(id=id, text=responses.pop(id, "<empty>"))
            for id in rest:
                try:
                    text = responses.pop(id, "<empty>")
                except KeyError:
                    continue
                item.add_sibling(SortedItem(id=id, text=text))
            yield item

    for id, text in responses.items():
        yield SortedItem(id, text)
