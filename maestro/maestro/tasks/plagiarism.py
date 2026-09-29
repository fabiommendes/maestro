from __future__ import annotations

import base64
import glob
import hashlib
from collections import defaultdict
from pathlib import Path
from typing import AsyncIterable, Callable, Iterable

import rich
import rich.panel
import rich.syntax
import trio
from pydantic import BaseModel, ConfigDict, Field, RootModel

from ..patterns import Pattern
from ..types import Hash, Loc, QuestionId, Repo, RepoId, RepoResponseSet, ResponseItem


#
# Auxiliary functions and classes
#
class DuplicateFile(BaseModel):
    """
    Represents a duplicate file in submissions.
    """

    model_config = ConfigDict(frozen=True)
    hash: str
    file: Path
    base: Path
    ids: frozenset[RepoId]

    def __post_init__(self):
        if len(self.ids) <= 1:
            raise ValueError("Duplicate must have at least two submissions.")

    def read_text(self, encoding: str = "utf-8") -> str:
        """
        Read the content of the duplicate file as text.
        """
        return self.base.read_text(encoding=encoding)

    def print_report(self):
        """
        Print a report of the duplicate file.
        """

        repos = ", ".join(f"[yellow]{repo_id}[/]" for repo_id in self.ids)
        data = rich.syntax.Syntax.from_path(self.base)

        rich.print(
            rich.panel.Panel(
                data,
                title=f"[b]Duplicate file [red]{self.file}[/][/] ([green]{self.hash}[/])",
                subtitle=repos,
                subtitle_align="left",
                title_align="left",
            )
        )


class DuplicateResponse(BaseModel):
    """
    Represents a duplicate response in submissions.
    """

    model_config = ConfigDict(frozen=True)
    question_id: QuestionId
    response: str
    ids: frozenset[RepoId]

    @property
    def hash(self) -> str:
        return base64.b64encode(self.response.encode("utf-8")).decode("ascii")

    def __post_init__(self):
        if len(self.ids) <= 1:
            raise ValueError("Duplicate response must have at least two repositories.")

    def print_report(self):
        """
        Print a report of the duplicate response.
        """

        repos = ", ".join(f"[yellow]{repo_id}[/]" for repo_id in self.ids)
        data = rich.syntax.Syntax(self.response, "markdown", word_wrap=True)
        rich.print(
            rich.panel.Panel(
                data,
                title=f"[b]Duplicate response [red]{self.question_id}[/][/] ([green]{self.hash}[/])",
                subtitle=repos,
                subtitle_align="left",
                title_align="left",
            )
        )


class Duplicates(RootModel):
    """
    Represents a collection of duplicate files and responses.
    """

    root: list[DuplicateFile | DuplicateResponse] = Field(default_factory=list)


async def find_duplicate_files(
    repos: list[Repo],
    template_folder: Path,
    require: list[Pattern],
) -> list[DuplicateFile]:
    """
    Find duplicate files in the list of repositories.
    """

    template_hashes = await compute_hashes(Repo(template_folder), require)
    template_prefix = str(template_folder.resolve())

    def ignore_template_symlinks(path: Path) -> bool:
        return path.is_symlink() and str(path.resolve()).startswith(template_prefix)

    duplicates = []
    async for duplicate in iter_duplicate_files(
        repos,
        patterns=require,
        ignore=ignore_template_symlinks,
        ignore_hashes=set(template_hashes.values()),
    ):
        duplicates.append(duplicate)
    return duplicates


async def iter_duplicate_files(
    repos: list[Repo],
    patterns: list[Pattern],
    ignore: Callable[[Path], bool] = lambda _: False,
    ignore_hashes: set[str] = set(),
) -> AsyncIterable[DuplicateFile]:
    """
    Find duplicate files.
    """
    type Key = tuple[Loc, Hash]
    hashes: dict[Key, list[tuple[RepoId, Path]]] = {}
    for repo in repos:
        for pattern in patterns:
            for file in pattern.paths(repo.path):
                if ignore(file) or not file.exists():
                    continue

                hash = await file_hash(file)
                if hash in ignore_hashes:
                    continue

                loc = file.relative_to(repo.path)
                repo_set = hashes.setdefault((Loc(loc), hash), [])
                repo_set.append((repo.path.name, file))

    for (loc, hash), ids in list(hashes.items()):
        if are_similar_ids(repo_id for repo_id, _ in ids):
            continue

        yield DuplicateFile(
            hash=hash,
            file=loc,
            base=ids[0][1],
            ids=frozenset(loc for loc, _ in ids),
        )


def find_duplicate_responses(responses: RepoResponseSet) -> list[DuplicateResponse]:
    copies: dict[tuple[QuestionId, ResponseItem], set[RepoId]] = defaultdict(set)
    for question_id, response_set in responses.items():
        for repo_id, response in response_set.items():
            copies[(question_id, response)].add(repo_id)

    duplicates = []
    for (question_id, response), repo_ids in copies.items():
        if are_similar_ids(repo_ids) or not response:
            continue

        duplicate = DuplicateResponse(
            question_id=question_id,
            response=response,
            ids=frozenset(repo_ids),  # type: ignore
        )
        duplicates.append(duplicate)
    return duplicates


def are_similar_ids(ids: Iterable[RepoId]) -> bool:
    """
    Check if set
    """
    return len({id.partition("@")[0] for id in ids}) <= 1


def get_immutable_paths(globs: list[str], base: Path) -> Iterable[Path]:
    """
    Get a list of immutable paths from the template directory.

    Args:
        globs:
            List of path globs to be treated as immutable.
        base:
            Path to the template directory.

    """
    paths = set()
    for pat in globs:
        paths.update(glob.glob(pat, root_dir=base, recursive=True))

    for name in sorted(paths):
        path = Path(name)
        if not path.is_dir():
            yield path


async def compute_hashes(
    repo: Repo,
    patterns: list[Pattern],
    ignore: Callable[[Path], bool] = lambda _: False,
) -> dict[Loc, Hash]:
    """
    Compute a mapping from locations files to their hashes in the repository.
    """
    hashes: dict[Loc, Hash] = {}
    for pattern in patterns:
        for file in pattern.paths(repo.path):
            if ignore(file):
                continue
            hash = await file_hash(file)
            loc = Loc(file.relative_to(repo.path))
            hashes[loc] = hash
    return hashes


async def file_hash(file: trio.Path | Path) -> Hash:
    """
    Compute the hash of a file.
    """
    if not isinstance(file, trio.Path):
        file = trio.Path(file)

    if not await file.is_file():
        raise ValueError(f"Path '{file}' is not a file.")

    async with await trio.open_file(file, "rb") as fd:
        return Hash(hashlib.md5(await fd.read()).hexdigest())
