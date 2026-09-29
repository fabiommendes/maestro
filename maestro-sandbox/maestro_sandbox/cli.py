from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer()


@app.command()
def run(
    path: Annotated[Path, typer.Argument(..., help="Path to the Python script.")],
) -> None:
    """Run a Python script in the sandbox."""
    pass


def main():
    app()
