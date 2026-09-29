from __future__ import print_function

import pytest
import rich

from .core import Config, Report, TestCase, grade


class MaestroPlugin:
    def __init__(self, config: pytest.Config | None = None):
        self.pytest_config = config
        cfg = Config.from_env()
        self.config = cfg.config
        self.grades = cfg.grades
        self.report = Report(user=self.config.user)

    def pytest_configure(self, config: pytest.Config):
        config.addinivalue_line(
            "markers", "grade(float): assign a grade different than 1.0 to the test."
        )
        if self.pytest_config is None:
            self.pytest_config = config
        if not hasattr(config, "_maestro"):
            self.pytest_config._maestro = self

    def pytest_addhooks(self, pluginmanager):
        pluginmanager.add_hookspecs(Hooks)

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_setup(self, item: pytest.Item):
        yield
        case = TestCase.from_item(item)
        self.report[case.pytest_id] = case

    def pytest_runtest_logreport(self, report: pytest.TestReport):
        case = self.report[report.nodeid]
        if report.when == "call":
            case.outcome = report.outcome
            case.duration = report.duration
            case.grade = grade(report.outcome, case.weight)
        return report

    @pytest.hookimpl(tryfirst=True)
    def pytest_sessionfinish(self, session):
        report = self.report

        n = len(report)
        weight = report.total_weight()
        grade = report.total_grade()
        rich.print(f"\n\nGraded {n} tests: {grade}/[white not bold]{weight}[/] points")

        if weight == grade:
            rich.print("[bold green]Congratulations![/]")
        else:
            rich.print("\nFailed tests")
            for case in report.values():
                if case.grade != case.weight:
                    color = "red" if case.grade == 0.0 else "yellow"
                    rich.print(f" - {case.id}: [{color}]{case.outcome}[/]")
                    if case.feedback:
                        rich.print(f"   [yellow]{case.feedback}[/]")


class Hooks:
    def pytest_maestro_modifyreport(self, maestro):
        """Called after building the final report and before saving it.

        Plugins can use this hook to modify the report before it's saved.
        """

    def pytest_maestro_runtest_metadata(self, item, call):
        """Return a dict which will be added to the current test item's
        metadata.

        Called from `pytest_runtest_makereport`. Plugins can use this hook to
        add metadata based on the current test run.
        """


@pytest.fixture
def json_metadata(request):
    """Fixture to add metadata to the current test item."""
    try:
        return request.node._maestro_extra.setdefault("metadata", {})
    except AttributeError:
        if not request.config.option.maestro:
            # The user didn't request a JSON report, so the plugin didn't
            # prepare a metadata context. We return a dummy dict, so the
            # fixture can be used as expected without causing internal errors.
            return {}
        raise


def pytest_addoption(parser):
    group = parser.getgroup("maestro", "emmit a grade for test results")
    group.addoption(
        "--maestro", default=False, action="store_true", help="create JSON report"
    )
    group.addoption(
        "--maestro-file",
        default="maestro-grades.json",
        # The case-insensitive string "none" will make the value None
        type=lambda x: None if x.lower() == "none" else x,
        help='target path to save the Grades report (use "none" to not save the report)',
    )


def pytest_configure(config):
    if not config.option.maestro:
        return
    plugin = MaestroPlugin(config)
    config._maestro = plugin
    config.pluginmanager.register(plugin)
    config.addinivalue_line(
        "markers", "grade(float): assign a grade different than 1.0 to the test."
    )


def pytest_unconfigure(config):
    plugin = getattr(config, "_maestro", None)
    if plugin is not None:
        del config._maestro
        config.pluginmanager.unregister(plugin)
