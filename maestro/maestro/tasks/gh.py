from __future__ import annotations

import shutil
import tempfile
from collections import defaultdict
from functools import partial
from logging import getLogger
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Literal,
    NamedTuple,
    NewType,
)

from ..errors import ConfigurationError
from ..notification import notify
from ..repo import PathMetadata, Repo
from .subprocess import Execution, create_subprocess, run_subprocess

if TYPE_CHECKING:
    pass

log = getLogger(__name__)


GithubClassroomId = NewType("GithubClassroomId", str)
GithubId = NewType("GithubId", str)


def github_classroom_id(value: str) -> GithubClassroomId:
    """
    Parse a string into a GithubClassroomId.

    Args:
        value (str): The string to convert.

    Returns:
        GithubClassroomId: The converted ID.
    """
    if not value.isdigit():
        raise ValueError(f"Invalid GitHub Classroom ID: {value!r}.")
    return GithubClassroomId(value)


def github_id(value: str) -> GithubId:
    """
    Parse a string into a GithubId/username.

    Args:
        value (str): The string to convert.

    Returns:
        GithubClassroomId: The converted ID.
    """
    if " " in value:
        raise ValueError(f"Invalid GitHub ID: {value!r}.")
    return GithubId(value)


class CloneContext(NamedTuple):
    skipped: list[str]
    cloned: list[str]


__all__ = [
    "GithubClassroomId",
    "GithubId",
    "fetch_github_classroom_template",
    "fetch_github_classroom_submissions",
    "github_classroom_id",
    "github_id",
]


async def fetch_github_classroom_template(
    id: GithubClassroomId,
    template_folder: Path,
    *,
    cache_folder: Path | None = None,
    notify: bool = False,
) -> Repo:
    """
    Clone a GitHub Classroom template repository.

    Args:
        id:
            The GitHub Classroom ID.
        cache_folder:
            The working directory to initiate the cloning process.
        template_folder:
            The directory to clone the template into.
        notify:
            Whether to notify the user about the cloning process.
    """
    if cache_folder is None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_folder = Path(tmpdir)
            return await fetch_github_classroom_template(
                id,
                template_folder=template_folder,
                cache_folder=cache_folder,
                notify=notify,
            )

    await gh_classroom_clone_command(id, at=cache_folder, template=True, notify=notify)
    return Repo(cache_folder)


async def fetch_github_classroom_submissions(
    ids: list[GithubClassroomId],
    submissions_folder: Path,
    cache_folder: Path | None = None,
    method: Literal["fast", "delete", "update"] = "fast",
    notify: bool = False,
    keep_repos: bool = False,
) -> list[Repo]:
    """
    Clone the GitHub Classroom student repositories.

    Args:
        ids:
            A list of GitHub Classroom IDs.
        cache_folder:
            The working directory to clone the template into.
        submissions_folder:
            The submissions will be cloned and moved to this folder after some
            clean up.
        method:
            The method to use for fetching submissions. Options are:
            - "fast": Re-use existing submissions if they exist and assume that
              they are up-to-date.
            - "update": Clone only new repositories, skipping existing ones.
            - "delete": Force cloning by deleting existing submissions.
        notify:
            Whether to notify the user about the cloning process.

    """
    if cache_folder is None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_folder = Path(tmpdir)
            return await fetch_github_classroom_submissions(
                ids,
                submissions_folder=submissions_folder,
                cache_folder=cache_folder,
                notify=notify,
            )

    # Check if submissions directory already exists and extract information to
    # make it reentrant-safe.
    if method == "fast" and has_submissions(submissions_folder):
        return [Repo(p) for p in submissions_folder.iterdir() if p.is_dir()]
    if method == "fast":
        method = "update"

    # Git clone submissions
    for id in ids:
        working_dir = cache_folder / "gh-classroom" / str(id)
        working_dir.mkdir(parents=True, exist_ok=True)

        # Remove old links in the temporary directory
        for tmp in working_dir.iterdir():
            for path in tmp.iterdir():
                if path.is_symlink():
                    path.unlink()

        await gh_classroom_clone_command(id, at=working_dir, notify=notify)

    for id in ids:
        working_dir = cache_folder / "gh-classroom" / str(id)
        clean_student_repos(
            id,
            working_dir,
            submissions_folder,
            method=method,
            keep_repos=keep_repos,
        )

    return [Repo(p) for p in submissions_folder.iterdir() if p.is_dir()]


def clean_student_repos(
    id: GithubClassroomId,
    working_dir: Path,
    submissions_folder: Path,
    method: Literal["delete", "update"] = "update",
    keep_repos: bool = False,
):
    """
    Process student repositories for GitHub Classroom.

    This function is a placeholder and should be implemented to handle
    the processing of student repositories as needed.
    """

    def copy_or_move(from_path: Path, to_path: Path):
        """
        Copy or move a repository from one path to another.
        """
        if not keep_repos:
            from_path.rename(to_path)
            backlink = to_path.parent.relative_to(from_path, walk_up=True)
            from_path.symlink_to(backlink, target_is_directory=True)
        else:
            shutil.copytree(from_path, to_path, dirs_exist_ok=True)

        # Update metadata
        meta = PathMetadata(to_path)
        meta.setdefault("gh-classroom-id", id)
        meta.setdefault("repo-source", f"gh-classroom:{id}")

    if method not in ("delete", "update"):
        raise ValueError(f"Unknown method: {method!r}")
    submissions_folder.mkdir(parents=True, exist_ok=True)

    # Get submissions dir
    paths_in_tmp_folder = {
        path
        for path in working_dir.iterdir()
        if path.is_dir() and path.name.endswith("-submissions")
    }
    if len(paths_in_tmp_folder) != 1:
        names = ", ".join(path.name for path in paths_in_tmp_folder)
        msg = f"Expected exactly one submissions directory, found: {names}"
        raise ValueError(msg)

    # Rename all directories to the submissions folder
    tmp_dir = paths_in_tmp_folder.pop()
    prefix = tmp_dir.name.removesuffix("submissions")
    submissions_folder.mkdir(parents=True, exist_ok=True)
    saved_ids = defaultdict(set)
    for p in submissions_folder.iterdir():
        if p.is_dir():
            repo_id, _, classroom_id = p.name.partition("@")
            saved_ids[repo_id].add(classroom_id)

    for path in tmp_dir.iterdir():
        if not path.is_dir():
            msg = f"Expected a directory in submissions, found: {path}"
            raise ConfigurationError(msg)
        if not path.name.startswith(prefix):
            msg = f"Invalid path name for github classroom submission: {path.name}, it should start with {prefix}"
            raise ConfigurationError(msg)
        if path.is_symlink():
            continue

        # We create a plan for moving the repository data to the destination
        # and moving any eventual existing repository to a new name.
        repo_id = github_id(path.name.removeprefix(prefix))

        # Repository do not exist in destination, we simply copy it
        if repo_id not in saved_ids:
            move_to = submissions_folder / repo_id
            if move_to.exists():
                msg = f"Overwriting existing submission {move_to} for {repo_id} in {id}"
                log.warning(msg)
            copy_or_move(path, move_to)
            continue

        # Repository exists in destination in a link <repo_id>@<id>
        ids = saved_ids[repo_id]
        if id in ids:
            if method == "update":
                log.debug(f"Skipping existing repository {repo_id} for {id}.")
                continue

            move_to = submissions_folder / f"{repo_id}@{id}"
            shutil.rmtree(move_to, ignore_errors=True)
            copy_or_move(path, move_to)
            continue

        # Repository exists without an specification of id
        if ids == {""}:
            destination = submissions_folder / repo_id
            meta = PathMetadata(destination)
            other_id = meta.get("gh-classroom-id")

            if id == other_id and method == "update":
                log.debug(f"Skipping existing repository {repo_id} for {id}.")
                continue
            elif id == other_id and method == "delete":
                move_to = submissions_folder / repo_id
                shutil.rmtree(move_to, ignore_errors=True)
                copy_or_move(path, move_to)
                continue
            elif id != other_id:
                move_to = submissions_folder / f"{repo_id}@{id}"
                copy_or_move(path, move_to)

                destination = submissions_folder / repo_id
                move_other_to = submissions_folder / f"{repo_id}@{other_id}"
                destination.rename(move_other_to)
                continue

        # Repository exists with other ids
        if ids:
            move_to = submissions_folder / f"{repo_id}@{id}"
            copy_or_move(path, move_to)
            continue

        raise RuntimeError("invalid program state")


async def gh_classroom_clone_command(
    id: GithubClassroomId,
    *,
    at: Path,
    template: bool = False,
    notify: bool = True,
) -> Execution:
    """
    Run the `gh classroom clone` command to clone a GitHub Classroom repository.
    """
    ctx = CloneContext([], [])
    gh_cmd = "starter-repo" if template else "student-repos"
    cmd = ["gh", "classroom", "clone", gh_cmd, "-a", str(id)]
    process = create_subprocess(cmd, at)
    on_line = partial(on_gh_classroom_line, ctx)
    return await run_subprocess(process, on_line)


def on_gh_classroom_line(ctx: CloneContext, line: str) -> None:
    """
    Handle a line read from the subprocess stdout.
    """
    skips = (
        line.startswith("Skip existing repo:")
        or line.startswith("Creating directory:")
        or line.startswith("Some repositories failed to clone.")
        and ctx.skipped
        or "--verbose flag" in line
        or line.startswith("Cloned ")
    )
    if skips:
        return

    if line.startswith("Error cloning "):
        _, _, line = line.partition("exists: ")
        dir = Path(line.strip())
        prefix = dir.parent.name.removesuffix("submissions")
        username = dir.name.removeprefix(prefix)
        ctx.skipped.append(username)
        return

    if line.startswith("Cloning into:"):
        line = line.removeprefix("Cloning into: ")
        dir = Path(line.strip())
        prefix = dir.parent.name.removesuffix("submissions")
        username = dir.name.removeprefix(prefix)

        ctx.cloned.append(username)
        notify(f"Cloned {username}")
        return

    if "no starter code" in line:
        raise ValueError("No starter code found in the repository.")

    raise AssertionError(f"invalid message: {line}")


def has_submissions(submissions_folder: Path) -> bool:
    """
    Check if the submissions folder contains any repositories.

    Args:
        submissions_folder (Path): The path to the submissions folder.

    Returns:
        bool: True if there are repositories, False otherwise.
    """
    if not submissions_folder.exists():
        return False
    if not submissions_folder.is_dir():
        raise ValueError(
            f"Expected a directory for submissions, found: {submissions_folder}"
        )

    for entry in submissions_folder.iterdir():
        if entry.is_dir() and not entry.name.startswith("."):
            return True

    return False
