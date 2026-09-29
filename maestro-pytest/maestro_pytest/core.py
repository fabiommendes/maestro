from __future__ import annotations

import os
from functools import singledispatch
from pathlib import Path
from typing import Annotated, Literal, MutableMapping, NamedTuple

import pytest
import toml
from pydantic import BaseModel as Model
from pydantic import Field
from slugify.slugify import slugify

type Id = str
type NodeId = str
type Outcome = Literal["passed", "failed", "skipped", "error", "xfailed", "xpassed"]


class Report(Model, MutableMapping[NodeId, "TestCase"]):
    user: str | None = None
    status: Literal["success", "error"] = "success"
    tests: Annotated[dict[NodeId, TestCase], Field(default_factory=dict)]

    def __getitem__(self, key):
        return self.tests[key]

    def __iter__(self):
        return iter(self.tests)

    def __len__(self):
        return len(self.tests)

    def __setitem__(self, key, value):
        self.tests[key] = value

    def __delitem__(self, key):
        del self.tests[key]

    def clear(self):
        self.tests.clear()

    def total_weight(self) -> float:
        return sum(case.weight for case in self.tests.values())

    def total_grade(self) -> float:
        return sum(case.grade for case in self.tests.values())


class TestCase(Model):
    id: Id
    group_id: Id
    pytest_id: NodeId
    outcome: Outcome = "error"
    duration: float = 0.0
    weight: float = 1.0
    grade: float = 0.0
    feedback: str | None = None

    @classmethod
    def from_item(cls, item: pytest.Item) -> "TestCase":
        weight, feedback = metadata(item)
        id = item_id(item)
        return cls(
            id=id,
            group_id=normalize_group_id(id),
            pytest_id=item.nodeid,
            weight=weight,
            feedback=feedback,
        )


class Config(Model):
    config: ConfigOptions
    grades: Annotated[ConfigGrades, Field(default_factory=lambda: ConfigGrades())]

    @classmethod
    def from_env(cls, base=Path(".")) -> "Config":
        config_data = None

        # Try load from maestro-test.toml, if it exists
        if (path := base / "maestro-test.toml").exists():
            config_data = toml.load(path)

        # If not found, try [tool.maestro-test] in pyproject.toml
        if config_data is None and (path := base / "pyproject.toml").exists():
            pyproject_data = toml.load(path)
            try:
                config_data = pyproject_data["tool"]["maestro-test"]
            except KeyError:
                pass

        # Finally, assume default data
        if config_data is None:
            config_data = {}

        options = config_data.setdefault("config", {})

        # Override from environment variables
        if "MAESTRO_USER" in os.environ:
            options["user"] = os.getenv("MAESTRO_USER")
        if "MAESTRO_ID" in os.environ:
            options["id"] = os.getenv("MAESTRO_ID")

        if "id" not in config_data:
            options["id"] = slugify(base.resolve().name)

        return cls.model_validate(config_data)


class ConfigGrades(Model):
    total: float = 100.0
    passed: float = 1.0
    failed: float = 0.0
    error: float = 0.0


class ConfigOptions(Model):
    id: str
    user: str | None = None


class Metadata(NamedTuple):
    weight: float = 1.0
    feedback: str = ""


#
# Item ID and weight logic
#


@singledispatch
def item_id(item: pytest.Item) -> str:
    """Generate a stable ID for a pytest item."""
    raise TypeError(item.nodeid, item.name, item)
    print(item, item.nodeid)
    return item.nodeid


@item_id.register
def _(item: pytest.Function) -> str:
    name = item.name.removeprefix("test_")
    return with_parent_id(name, item.parent)


@item_id.register
def _(item: pytest.Module) -> str:
    name = item.name.removesuffix(".py").removeprefix("test_")
    return with_parent_id(name, item.parent)


@item_id.register
def _(item: pytest.Class) -> str:
    return with_parent_id(item.name.removeprefix("Test") + ":", item.parent)


@item_id.register
def _(item: pytest.Dir) -> str:
    if item.name == "tests":
        return ""
    return with_parent_id(item.name, item.parent)


def with_parent_id(id: str, parent: pytest.Item | None) -> str:
    if parent is None:
        return id
    parent_id = item_id(parent)
    if parent_id == "":
        return id
    if parent_id[-1] in (".", "/", ":"):
        return parent_id + id
    return parent_id + "." + id


def metadata(item: pytest.Item) -> Metadata:
    """Get the weight/feedback of a pytest item."""
    mark = item.get_closest_marker("grade")
    if mark is None:
        return Metadata()
    return Metadata(
        weight=float(mark.args[0]),
        feedback=mark.kwargs.get("feedback", ""),
    )


def grade(outcome: Outcome, /, weight: float = 1.0) -> float:
    if outcome in ("passed", "xpassed", "xfailed"):
        return weight
    return 0.0


def normalize_group_id(id: str) -> str:
    """Normalize a test case ID to a group ID by removing any trailing
    parameters.

    Examples:
        "test_func[param]" -> "test_func"
    """
    if id.endswith("]"):
        return id.rpartition("[")[0]
    return id
