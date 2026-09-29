from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from fractions import Fraction
from pathlib import Path

import trio

from ..types import Grade
from ..utils import camel_case_to_snake_case


async def parse_report(report_json: Path, /) -> dict[str, float]:
    """
    Read grades from the pytest report.json file.

    Args:
        report_json: The path to the report.json file.
    """

    async with await trio.open_file(report_json) as fd:
        report = json.loads(await fd.read())

    grades: dict[str, TestModule] = {}
    for test in report["tests"]:
        id = NodeId.parse(test["nodeid"])
        outcome = test["outcome"]
        if outcome == "skipped":
            continue
        if outcome not in ("passed", "failed", "error"):
            raise ValueError(f"Unexpected outcome: {outcome!r} for test {id}")

        if id.module not in grades:
            mod = grades[id.module] = TestModule(id.module)
        else:
            mod = grades[id.module]

        test_case = mod.get_case(id.test, id.cls)
        test_case.set_success(test["outcome"] == "passed", id.example)

    return {key: mod.mean_grade() for key, mod in grades.items()}


@dataclass
class NodeId:
    """
    Parsed form of a node id string in the format 'module::Class::test[example]'.
    """

    module: str
    test: str
    cls: str | None
    example: str | None

    @staticmethod
    def parse(nodeid: str) -> NodeId:
        """
        Parse the node ID to extract the module, class, test, and example.
        """
        cls = None
        example = None
        module, _, test = nodeid.partition("::")

        # Normalize the module name
        module = module.removeprefix("tests/")
        parts = module.split(os.path.sep)
        parts[-1] = parts[-1].removeprefix("test").removeprefix("_")
        module = ".".join(parts)
        module = module.removesuffix(".py").replace(os.path.sep, ".").replace("_", "-")

        # Normalize the example name
        if test.endswith("]"):
            test, _, example = test.partition("[")
            example = example.removesuffix("]")

        # Extract and normalize the class name
        if "::" in test:
            cls, _, test = test.partition("::")
            cls = cls.removeprefix("test").removeprefix("_")
            cls = cls.removeprefix("Test").replace("_", "-")
            cls = camel_case_to_snake_case(cls, sep="-")

        # Normalize the test name
        test = test.removeprefix("test").removeprefix("_")
        test = test.replace("_", "-")

        return NodeId(module, test, cls, example)


@dataclass
class TestModule:
    name: str
    data: list[TestCase] = field(default_factory=list)
    weight: Grade = Grade(1)

    def mean_grade(self) -> Grade:
        """
        Get the mean grade of the test module.
        """
        acc = Grade(0)
        total = Grade(0)

        for case in self.data:
            acc = Grade(acc + case.weight * case.mean_grade())
            total = Grade(total + case.weight)

        assert acc <= total, "acc should not be greater than total"
        return Grade(acc / total)

    def get_case(self, test: str, cls: str | None) -> TestCase:
        """
        Get a test case by its name.
        """
        for case in self.data:
            if case.name == test and case.cls == cls:
                return case

        case = TestCase(test, cls)
        self.data.append(case)
        return case


@dataclass
class TestCase:
    name: str
    cls: str | None = None
    success: bool | None = None
    weight: Fraction = Fraction(1)
    examples: dict[str, bool] = field(default_factory=dict)
    weights: dict[str, Fraction] = field(default_factory=dict)

    def set_success(self, success: bool, example: str | None = None) -> None:
        """
        Set the success status of the test case.
        """
        if example is None and self.examples:
            msg = "cannot assign full success/failure with a test case with partial successes"
            raise ValueError(msg)
        if example is not None and self.success is not None:
            msg = "cannot assign partial success/failture with a test case without examples"
            raise ValueError(msg)

        if example is None:
            self.success = success
        else:
            if example in self.examples:
                raise ValueError(f"Example {example} already exists in {self.name}")
            self.examples[example] = success

    def mean_grade(self) -> Fraction:
        """
        Get the mean grade of the test case.
        """
        if self.success is not None:
            return Fraction(1) if self.success else Fraction(0)

        acc = Fraction(0)
        total = Fraction(0)

        for example, success in self.examples.items():
            weight = self.weights.get(example, Fraction(1))
            acc += weight * (Fraction(1) if success else Fraction(0))
            total += weight

        assert acc <= total, "acc should not be greater than total"
        return acc / total if total > 0 else Fraction(0)
