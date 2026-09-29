import shutil
import sys
from ast import Not
from functools import partial
from pathlib import Path
from typing import Annotated, Literal, cast

import rich
import trio
import typer

from .. import errors
from ..activity import Repository
from ..classroom import Classroom
from ..repo import Repo
from .activities import CreateActivity, ListActivities
from .app import MaestroApp
from .grading import Grading
from .students import ListStudents

__all__ = [
    "main",
    "app",
    "MaestroApp",
    "ListActivities",
    "CreateActivity",
    "Grading",
    "ListStudents",
]

app = typer.Typer(
    name="maestro",
    pretty_exceptions_show_locals=False,
    help="Maestro CLI for managing classroom activities and students.",
)

ACTIVITY_ARG = Annotated[
    str,
    typer.Argument(
        help="A valid activity in the classroom.",
    ),
]
DEBUG_OPT = Annotated[
    bool,
    typer.Option(
        help="Turn debugging on",
    ),
]
FORCE_OPT = Annotated[
    bool,
    typer.Option(help="Ignore results in cache."),
]
OPTIONAL_SUBMISSION = Annotated[
    str | None,
    typer.Option(
        ...,
        "-s",
        "--submission",
        help="An specific submission. If not given, apply to all submissions.",
    ),
]


@app.callback(invoke_without_command=True)
def default():
    if len(sys.argv) == 1:
        ui()


@app.command(rich_help_panel="Terminal interaction")
def pipeline(
    activity: ACTIVITY_ARG,
    skip_init: Annotated[
        bool,
        typer.Option(
            help="Skip initial configuration and image creation",
        ),
    ] = False,
    debug: DEBUG_OPT = False,
    skip_fetch: bool = False,
    skip_autograde: bool = False,
    skip_clean: bool = False,
):
    """
    Run the full grading pipeline for the specified activity.

    Args:
        activity: The name of the pipeline activity to run.
    """
    classroom = get_classroom()
    task = classroom.get_activity(activity)
    task.run(
        skip_init=skip_init,
        skip_fetch=skip_fetch,
        skip_autograde=skip_autograde,
        skip_clean=skip_clean,
    )


@app.command(rich_help_panel="Terminal interaction")
def clean(
    activity: ACTIVITY_ARG,
    step: Annotated[
        str,
        typer.Argument(
            help="Options are: 'all', 'links', 'autograde'.",
            case_sensitive=False,
        ),
    ] = "all",
    submission: OPTIONAL_SUBMISSION = None,
):
    """
    Clean the specified activity.
    """
    classroom = get_classroom()
    task = classroom.get_activity(activity)
    repo = submission

    async def clean(step: str):
        if not isinstance(task, Repository):
            rich.print(f"[b red]Activity '{activity}' is not a repository activity.")
            exit(1)

        match step:
            case "links":
                await task.clean_template_links(repo)
            case "annotations":
                await task.clean_annotations(repo)
            case "files":
                await task.clean_files(repo)
            case _:
                rich.print(f"[b red]Unknown step '{step}'.")

    # Repository-specific cleaning
    step = step.lower()
    if step == "all":
        for step in ["links", "files"]:
            trio.run(clean, step)
    else:
        trio.run(clean, step)


@app.command(rich_help_panel="Terminal interaction")
def update(
    activity: ACTIVITY_ARG,
    force: Annotated[
        bool,
        typer.Option(help="Pull repositories again, even when cached"),
    ] = False,
    submission: OPTIONAL_SUBMISSION = None,
):
    """
    Update submissions for the given activity.
    """
    classroom = get_classroom()
    task = classroom.get_activity(activity)

    if isinstance(task, Repository):
        fn = partial(task.fetch_submissions, force=force)
        trio.run(fn)
    else:
        raise NotImplementedError


@app.command(rich_help_panel="Terminal interaction")
def plagiarism(
    activity: ACTIVITY_ARG,
    force: FORCE_OPT = False,
):
    """
    Find cases of plagiarism in the specified activity.
    """
    classroom = get_classroom()
    task = classroom.get_activity(activity)

    if isinstance(task, Repository):
        fn = partial(task.plagiarism_pipeline, force=force)
        trio.run(fn)
    else:
        raise NotImplementedError


@app.command(rich_help_panel="Terminal interaction")
def grades(
    activity: ACTIVITY_ARG,
    skip_plagiarism: bool = False,
    summary: Annotated[
        bool,
        typer.Option(
            ...,
            "-s",
            help="Show a summary of the grades instead of the full table.",
        ),
    ] = False,
    output: Annotated[
        Path | None,
        typer.Option(
            ...,
            "-o",
            help="Save to a file instead of printing to the console.",
        ),
    ] = None,
):
    """
    Show the grades for the specified activity.
    """
    classroom = get_classroom()
    task = classroom.get_activity(activity)

    if isinstance(task, Repository):
        fn = partial(task.grading_pipeline, skip_plagiarism=skip_plagiarism)
        df = trio.run(fn)
    else:
        raise NotImplementedError

    if summary:
        rich.print(df.describe().T[["min", "mean", "50%", "max", "std"]])
    elif output is None:
        rich.print(df.to_csv())
    elif output.suffix == ".csv":
        df.to_csv(output)
    elif output.suffix in (".xls", ".xlsx"):
        df.to_csv(output)
    else:
        msg = f"[b red]Unsupported output format: {output.suffix}. Use .csv or .xlsx."
        rich.print(msg)


@app.command(rich_help_panel="Terminal interaction")
def transform(
    activity: ACTIVITY_ARG,
):
    """
    Show the grades for the specified activity.
    """
    classroom = get_classroom()
    task = classroom.get_activity(activity)

    if isinstance(task, Repository):
        fn = partial(task.transform_pipeline)
        rich.print(trio.run(fn))
    else:
        raise NotImplementedError


@app.command(rich_help_panel="Terminal interaction")
def run(
    activity: ACTIVITY_ARG,
    submission: str,
):
    """
    Run some activity
    """
    classroom = get_classroom()
    task = classroom.get_activity(activity)

    if isinstance(task, Repository):
        repo = task.get_repo(submission)
        fn = partial(task.autograde_submission, force=True)
        rich.print(trio.run(fn, repo))
    else:
        raise NotImplementedError


@app.command(rich_help_panel="Terminal interaction")
def rm(
    activity: ACTIVITY_ARG,
    file: str,
):
    """
    Remove files in activity
    """
    classroom = get_classroom()
    task = classroom.get_activity(activity)

    if isinstance(task, Repository):

        async def run():
            async for repo in task.repositories():
                path = trio.Path(repo.path / file)
                if (exists := await path.exists()) and await path.is_dir():
                    shutil.rmtree(Path(path), ignore_errors=True)
                elif exists:
                    await path.unlink()

        trio.run(run)
    else:
        raise NotImplementedError


@app.command(rich_help_panel="Terminal interaction")
def missing(
    activity: ACTIVITY_ARG,
    file: str,
):
    """
    Remove files in activity
    """
    classroom = get_classroom()
    task = classroom.get_activity(activity)

    if isinstance(task, Repository):

        async def run():
            repos = []
            async for repo in task.repositories():
                path = trio.Path(repo.path / file)
                if not await path.exists():
                    repos.append(repo.id)

            if repos:
                rich.print(f"[b red]Missing file: {file}")
            for id in sorted(repos, key=lambda p: str(p).lower()):
                rich.print(f" - [b]{id}")

        trio.run(run)
    else:
        raise NotImplementedError


@app.command(rich_help_panel="Maestro TUI")
def ui():
    """
    Run the Maestro application with the specified configuration file.
    """
    app = MaestroApp(get_classroom())
    app.run()


def get_classroom():
    path = Path.cwd()
    for path in [path, *path.parents]:
        if (path / Classroom.CONFIG_FILE).exists():
            break
    else:
        msg = f"Could not find {Classroom.CONFIG_FILE} in the current directory or any parent directory."
        raise FileNotFoundError(msg)

    classroom = Classroom(path=path)
    classroom.prepare_folders()
    return classroom


def main():
    """
    Main entry point for the Maestro CLI.
    """
    try:
        app()
    except errors.UserFacingError as e:
        e.print()
        exit(e.error_code)
