from pathlib import Path
from typing import Annotated

import pytest
import toml
import typer

from .util import slugify, toml_escape

app = typer.Typer(
    pretty_exceptions_short=True,
    pretty_exceptions_show_locals=False,
)

CONFIG_TEMPLATE = """[{prefix}config]
id = {slug}

[{prefix}grades]
total = 100.0
passed = 1.0
failed = 0.0
error = 0.0
"""


@app.command()
def init(
    slug: Annotated[
        str | None,
        typer.Option(
            "--id",
            "-i",
            help="A unique identifier for this test suite, e.g. 'intro-to-python'",
        ),
    ] = None,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Overwrite existing maestro-test.toml if it exists",
            is_flag=True,
        ),
    ] = False,
):
    """
    Initialize a maestro-pytest configuration file in the current directory.
    """
    if slug is None:
        slug = slugify(Path(".").resolve().name)

    if force:
        with open("maestro-test.toml", "w") as f:
            f.write(generate_config(slug=slug))
            return

    if Path("maestro-test.toml").exists():
        typer.echo("maestro-test.toml already exists. Use --force to overwrite.")
        raise typer.Exit(code=1)

    if (path := Path("pyproject.toml")).exists():
        config_data = toml.loads(config_source := path.read_text())

        if "tool" in config_data and "maestro-test" in config_data["tool"]:
            typer.echo("pyproject.toml is already configured for maestro-test.")
            raise typer.Exit(code=1)

        config_source += "\n\n[tool.maestro-test]\n"
        config_source += generate_config(slug=slug, prefix="tool.maestro-test")
        path.write_text(config_source)

    else:
        with open("maestro-test.toml", "w") as f:
            f.write(generate_config(slug=slug))


@app.command()
def run():
    """
    Run tests with pytest and collect results for Maestro.
    """

    pytest_args = ["--maestro", "maestro-report.json"]
    pytest_exit_code = pytest.main(pytest_args)
    raise typer.Exit(code=pytest_exit_code)


def main():
    app()


def generate_config(slug: str, prefix="") -> str:
    if prefix and not prefix.endswith("."):
        prefix += "."
    return CONFIG_TEMPLATE.format(slug=toml_escape(slug), prefix=prefix)
