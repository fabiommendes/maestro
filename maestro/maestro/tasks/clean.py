from __future__ import annotations

import shutil
from collections import defaultdict
from functools import partial
from pathlib import Path
from typing import (
    Callable,
    Iterable,
    Mapping,
    NamedTuple,
    Protocol,
    cast,
)

import trio
from returns import result

from ..errors import ConfigurationError
from ..patterns import Dir, File, Glob, Pattern, keep_files
from ..repo import Repo, RepoError
from ..types import Hash
from ..utils import function_name
from .plagiarism import file_hash

NOT_GIVEN = NotImplemented
DEDUP_CACHE: dict[tuple[Path, frozenset[Pattern]], DedupCtx] = {}
type DedupCtx = defaultdict[str, dict[Path, Hash]]

__all__ = [
    "dedup",
    "remove",
    "protect",
    "validate",
]


async def dedup(
    repo: Repo,
    /,
    template: Path,
    cache: DedupCtx = NOT_GIVEN,
    no_dedup: Iterable[Pattern] = (),
    force: bool = False,
) -> None:
    """
    Remove duplicate files and link to the corresponding ones in the template
    directory.
    """
    if not force and repo.meta.get("dedup-files", None) == repo.b64digest():
        return

    if cache is NOT_GIVEN:
        cache = await dedup_cache(template, frozenset(no_dedup))

    for path, _, filenames in repo.path.walk(follow_symlinks=False):
        for name in filenames:
            if name not in cache:
                continue

            file = path / name
            if not file.is_file(follow_symlinks=False):
                continue

            data = await file_hash(file)
            for key, memoized in cache.get(name, {}).items():
                if data == memoized:
                    target = template / key
                    link_to_template(file, target)
                    break

    repo.meta["dedup-files"] = repo.b64digest()


async def dedup_cache(template: Path, no_dedup: frozenset[Pattern]) -> DedupCtx:
    """
    Read the template repository and store a copy of all its files.
    """
    await trio.sleep(0)
    if (template, no_dedup) in DEDUP_CACHE:
        return DEDUP_CACHE[(template, no_dedup)]
    if len(DEDUP_CACHE) >= 5:
        DEDUP_CACHE.popitem()

    ignore_dirs = set()
    ignore_files = set()
    ignore_globs = set()

    for pattern in no_dedup:
        if isinstance(pattern, Dir):
            ignore_dirs.add(pattern.data)
        elif isinstance(pattern, File):
            ignore_files.add(pattern.data)
        elif isinstance(pattern, Glob):
            ignore_globs.add(pattern.data)
        else:
            raise TypeError(f"Invalid pattern type: {type(pattern)}")

    # Create a mapping from file names to the list of memoized pairs of (path, data).
    memo_names: DedupCtx = defaultdict(dict)
    DEDUP_CACHE[(template, no_dedup)] = memo_names

    for path, dirnames, filenames in template.walk():
        dirs = {(path / name).relative_to(template) for name in dirnames}
        dirs -= ignore_dirs
        remove_globs(dirs, ignore_globs)
        dirnames[:] = [path.name for path in dirs]

        files = {(path / name).relative_to(template) for name in filenames}
        files -= ignore_files
        remove_globs(dirs, ignore_globs)

        for path in files:
            file = trio.Path(template / path)
            if await file.is_file(follow_symlinks=False):
                data = await file_hash(file)

            memo_names[path.name][path] = data
    return memo_names


async def validate(
    repo: Repo,
    /,
    *,
    required: Iterable[Pattern],
    link_to: Path | None = None,
    force: bool = False,
) -> result.Result[Repo, RepoError]:
    """
    Validate the files in the student submission repositories against the
    specified patterns.

    Args:
        repo:
            The path to the student submission repository.
        required:
            A list of patterns that must be present in the repository.
        link_to:
            If provided, the repository will be linked to a folder with all
            invalid files. This is useful for debugging purposes.
    """
    missing = set[Path]()
    found = set[Path]()
    if not force and repo.meta.get("validate-files", None) == repo.b64digest():
        if (missing := repo.meta.get("missing-files", None)) is None:
            return result.Success(repo)
        missing = set(Path(p) for p in missing)
        await link_invalid(link_to, repo)
        return RepoError.missing_files(repo, list(missing)).as_failure()

    for pattern in required:
        match pattern:
            case Dir(path) | File(path):
                path = repo.path / path
                if path.exists() and not is_external_symlink(path, repo.path):
                    found.add(path.relative_to(repo.path))
                else:
                    missing.add(path.relative_to(repo.path))
            case Glob(glob):
                msg = f"Glob patterns are not supported: {glob}"
                raise ConfigurationError(msg)

    repo.meta["validate-files"] = repo.b64digest()
    if missing:
        await link_invalid(link_to, repo)
        repo.meta["missing-files"] = sorted(map(str, missing))
        return RepoError.missing_files(repo, list(missing)).as_failure()

    return result.Success(repo)


async def link_invalid(
    link_to: Path | None,
    repo: Repo,
) -> None:
    if link_to is None:
        return await trio.sleep(0)

    alink_to = trio.Path(link_to)
    await alink_to.mkdir(parents=True, exist_ok=True)
    alink_path = trio.Path(link_to / repo.path.name)
    link_dest = repo.path.relative_to(link_to, walk_up=True)
    if not await alink_path.exists():
        await alink_path.symlink_to(link_dest, target_is_directory=True)


async def remove(
    repo: Repo,
    exclude: Iterable[Pattern],
    keep: list[Pattern],
    force: bool = False,
) -> None:
    """
    Exclude certain patterns from the repository path.

    This is useful for ignoring certain directories or files.

    Args:
        repo:
            The path to the student submission repository.
        exclude:
            A list of patterns to exclude from the repository.
        keep:
            A list of patterns to keep in the repository. It has priority over `exclude`.
        force:
            If True, the function will re-run, even if the repository is in cache.

    This function is re-entrant because exclusion is idempotent.
    """
    if not force and repo.meta.get("exclude-files", None) == repo.b64digest():
        return

    await path_action(
        repo=repo.path,
        patterns=exclude,
        skip=keep,
        on_path=force_remove_path,
    )

    repo.invalidate_hash_cache()
    repo.meta["exclude-files"] = repo.b64digest()


async def protect(
    repo: Repo,
    /,
    *,
    template: Path,
    protect: Iterable[Pattern],
    keep: Iterable[Pattern],
    force: bool = False,
) -> None:
    """
    Protect certain patterns from the activity path.

    Those paths are removed from the repository and linked to the template
    directory files. This is useful for reverting changes that might be made
    to test files, data files, examples, etc.

    Args:
        activity:
            The repository activity.
        repo:
            The path to the student submission repository.
        protect:
            A list of patterns to protect in the repository. Those are linked to
            the template directory files.
        keep:
            A list of patterns to keep in the repository, even when they match
            a pattern in the protect list. It has priority over `protect`.
    """
    if not force and repo.meta.get("protect-files", None) == repo.b64digest():
        return

    await path_action(
        repo=repo.path,
        patterns=protect,
        skip=keep,
        on_path=partial(normalized_protected_path, repo=repo.path),
        template=template,
    )

    repo.meta["protect-files"] = repo.b64digest()


def is_external_symlink(path: Path, base: Path) -> bool:
    """
    True if path is a symlink back to some repository external to base.
    """
    if not path.is_symlink():
        return False
    try:
        path.resolve().relative_to(base, walk_up=False)
    except ValueError:
        return True
    else:
        return False


async def path_action[**P](
    repo: Path,
    patterns: Iterable[Pattern] = [],
    skip: Iterable[Pattern] = [],
    on_path: _OnPathFn[P] | None = None,
    *args: P.args,
    **extra: P.kwargs,
) -> None:
    """
    Execute function on each path covered by the given patterns

    Args:
        repo:
            Base path for the repository
        patterns:
            List of path patterns to search.
        exclude:
            List of patterns excluded from the transformation.
        on_path:
            Callback function called for each found path not in exclude.
        **extra:
            Arbitrary keyword arguments passed to the `on_path` function.
    """
    kept_files = keep_files(repo, skip)

    for pattern in patterns:
        match pattern:
            case Dir(path) | File(path):
                path = repo / path
                if path in kept_files:
                    continue
                if on_path is not None:
                    await on_path(path, *args, **extra)

            case Glob(glob):
                for path in repo.glob(glob):
                    if path in kept_files:
                        continue
                    if on_path is not None:
                        await on_path(path, *args, **extra)

            case _:
                raise TypeError(f"Invalid pattern type: {type(pattern)}")


def remove_globs(paths: set[Path], globs: set[str]) -> None:
    """
    Remove paths that match any of the provided glob patterns.
    """
    for glob in globs:
        for path in paths.copy():
            if path.match(glob):
                paths.remove(path)


def link_to_template(path: Path, to: Path):
    destination_link = to.relative_to(path.parent, walk_up=True)
    if path.exists(follow_symlinks=False):
        path.unlink()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.symlink_to(destination_link)


async def force_remove_path(path: Path, check_exists: bool = False) -> None:
    """
    Forcefully remove a file or directory, ignoring errors.

    Args:
        path (Path): The path to the file or directory to remove.
    """
    if check_exists and not path.exists():
        raise FileNotFoundError(f"Path {path} does not exist.")
    elif not path.exists():
        return
    if path.is_dir(follow_symlinks=False):
        shutil.rmtree(path, ignore_errors=True)
    else:
        path.unlink(missing_ok=True)


async def normalized_protected_path(path: Path, *, repo: Path, template: Path) -> None:
    """
    Link a path from the repo repository to the corresponding file in the
    template directory.
    """
    key = path.relative_to(repo)
    template_path = template / key
    await force_remove_path(path)

    if not template_path.exists():
        return

    symlink = template_path.relative_to(path.parent, walk_up=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.symlink_to(symlink, target_is_directory=template_path.is_dir())


class DedupContext(NamedTuple):
    memo: Mapping[str, list[tuple[Path, bytes]]]
    repo: Path
    template: Path


class LazyCtx[A, T]:
    """
    Lazy context that allows to defer the evaluation of the context until
    it is actually needed.
    """

    def __init__(self, start: Callable[[A], T]):
        self._activity: A | None = None
        self._value: T | None = None
        self._factory = start

    def get(self, activity: A) -> T:
        """
        Get the context value for the given activity.
        If the context is not yet evaluated, it will be evaluated now.
        """
        if self._activity is None:
            self._activity = activity
            self._value = self._factory(activity)
            return self._value
        elif self._activity is not activity:
            msg = f"Context already evaluated for a different activity: {self._activity} != {activity}"
            raise ConfigurationError(msg)
        return cast("T", self._value)

    def __repr__(self):
        return function_name(self._factory)


class _OnPathFn[**P](Protocol):
    async def __call__(
        self, path: Path, /, *args: P.args, **kwargs: P.kwargs
    ) -> None: ...
