from __future__ import annotations

import base64
import enum
import hashlib
import time
from contextlib import contextmanager
from dataclasses import field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator, Literal, MutableMapping, NewType

from returns import result
from ruamel.yaml.comments import CommentedMap

from .yaml import yaml

EXPIRE_TIME = 5 * 60  # 5 minutes in seconds
MAX_CACHE_SIZE = 512
HASHES: dict[Path, tuple[bytes, float]] = {}
type YAML = CommentedMap

__all__ = [
    "Repo",
    "RepoId",
    "RepoError",
    "AnnotatedPath",
    "PathMetadata",
    "b64digest",
    "digest",
    "hexdigest",
    "repo_id",
]


#: Repository identifier. Locate each repository by its ID.
RepoId = NewType("RepoId", str)


def repo_id(id: str) -> RepoId:
    """
    Create a RepoId from a string.
    """
    return RepoId(id)


def delegate(name: str, read_only: bool = False):
    if read_only:

        def delegate(self, *args, **kwargs):
            with self._read_only_db() as db:
                method = getattr(db, name)
                return method(*args, **kwargs)

    else:

        def delegate(self, *args, **kwargs):
            with self._write_db() as db:
                method = getattr(db, name)
                return method(*args, **kwargs)

    delegate.__name__ = name
    return delegate


# TODO: remove this and change to a PathAnnotations mapping
class AnnotatedPath[V = Any, K = str](Path, MutableMapping[K, V]):
    __slots__ = ("_db_cache",)
    _db_cache: None | CommentedMap

    @property
    def path(self):
        return Path(str(self))

    def __init__(self, *args: Path | str):
        super().__init__(*args)
        self._db_cache = None

    def log(
        self,
        key: str,
        data: str,
        *,
        mode: Literal["w", "a"] = "w",
        at: Path | str | None = None,
    ) -> None:
        """
        Log a message to the repository's log file.

        Args:
            key: The key to log.
            value: The value to log.
        """
        log_file = self._log_file(key, at)
        if mode == "w":
            log_file.write_text(data, encoding="utf-8")
        else:
            with log_file.open("a") as fd:
                fd.write(data)

    def log_append(self, key: str, data: str, *, at: Path | str | None = None) -> None:
        """
        Log a message to the repository's log file.

        Args:
            key: The key to log.
            value: The value to log.
        """
        if self._log_file(key, at).exists():
            self.log(key, "\n\n" + data, mode="a")
        else:
            self.log(key, data, mode="w")

    def _log_file(self, key: str, sub_path: Path | str | None = None) -> Path:
        """
        Get the log file path for the given key.
        """
        if sub_path is not None:
            return self / sub_path / f".maestro-{key}.log"
        return self / f".maestro-{key}.log"

    @contextmanager
    def _write_db(self, *, _save: bool = True):
        file = self / ".maestro.yaml"
        try:
            if self._db_cache is not None:
                data = self._db_cache
            elif file.exists():
                source = file.read_text(encoding="utf-8")
                self._db_cache = data = yaml.load(source) or CommentedMap()
                assert isinstance(data, CommentedMap), data
            else:
                self._db_cache = data = CommentedMap()
            yield data
        finally:
            if _save:
                yaml.dump(data, file)

    def _read_only_db(self):
        return self._write_db(_save=False)

    def __delitem__(self, key: K):
        with self._write_db() as db:
            del db[key]

    def __getitem__(self, key: K):
        with self._read_only_db() as db:
            return db[key]

    def __setitem__(self, key: K, value: V):
        with self._write_db() as db:
            db[key] = value

    def __iter__(self) -> Iterator[K]:
        with self._read_only_db() as db:
            return iter(db)

    def __len__(self) -> int:
        with self._read_only_db() as db:
            return len(db)

    if not TYPE_CHECKING:
        update = delegate("update")
        keys = delegate("keys")
        values = delegate("values")
        items = delegate("items")
        setdefault = delegate("setdefault")


class Repo:
    _data: AnnotatedPath
    _hash: bytes = field(default=b"", repr=False)

    @property
    def path(self):
        return self._data.path

    @property
    def id(self) -> RepoId:
        return repo_id(self.path.name)

    @property
    def is_template(self):
        if self.path.parent.name == "submissions":
            return False
        return self.path.name == "template"

    @property
    def meta(self):
        return self._data

    def __init__(self, path: Path | str):
        if isinstance(path, str):
            path = Path(path)

        # Repos must refer to real paths
        assert isinstance(path, Path), path
        if not path.exists():
            raise ValueError(f"Repository path {path} does not exist")

        # We expect repos to be either in the "submissions" folder or it to
        # be a template path
        if not (path.parent.name == "submissions" or path.name == "template"):
            msg = f"Not a valid repository path: {path}"
            raise ValueError(msg)
        self._data = AnnotatedPath(path)

    def __str__(self):
        if self.is_template:
            return str(self.path.relative_to(self.path.parent.parent.parent))
        else:
            return str(self.path.relative_to(self.path.parent.parent))

    def __hash__(self):
        return hash((Repo, self._data.path))

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self.path)!r})"

    def digest(self, force: bool = False) -> bytes:
        """
        Get the hash key of the repository files.

        This is cached to avoid costly computations, unless force=True.
        """
        hash, t = get_hash(self.path)
        if time.time() < t + EXPIRE_TIME and not force:
            self._hash = hash
        else:
            self._hash = compute_hash(self.path)
        return self._hash

    def hexdigest(self, force: bool = False) -> str:
        """
        Get the hash of the repository as a hexadecimal string.
        """
        return self.digest(force).hex()

    def b64digest(self, force: bool = False) -> str:
        """
        The base64 digest of the hash string.
        """
        return base64.b64encode(self.digest(force)).decode("ascii")

    def is_updated(self, force: bool = False) -> bool:
        """
        Return true if the current stored hash is the same as the computed one
        in the repository.

        This method avoid re-computing the hash if the last computation is
        sufficiently recent. If force=True, then it always re-ccompute
        the hash.
        """
        if self.path not in HASHES:
            if force:
                self.digest(force)
            return True
        hash, _ = HASHES[self.path]
        return self.digest(force) == hash

    def invalidate_hash_cache(self) -> None:
        """
        Clean the hash cache for this repository.
        """
        HASHES.pop(self.path, None)

    def log(self, key: str, data: str, mode: Literal["w", "a"] = "w") -> None:
        """
        Log a message to the repository's log file.

        Args:
            key: The key to log.
            value: The value to log.
        """
        self.meta.log(key, data, mode=mode)


class PathMetadata[V = Any, K = str](MutableMapping[K, V]):
    __slots__ = ("_path", "_db_cache")
    _db_cache: None | CommentedMap

    @property
    def path(self) -> Path:
        return Path(self._path)

    def __init__(self, path=Path | str):
        if isinstance(path, str):
            path = Path(path)
        self._path = path
        self._db_cache = None

    def log(
        self,
        key: str,
        data: str,
        *,
        mode: Literal["w", "a"] = "w",
        at: Path | str | None = None,
    ) -> None:
        """
        Log a message to the repository's log file.

        Args:
            key: The key to log.
            value: The value to log.
        """
        log_file = self._log_file(key, at)
        if mode == "w":
            log_file.write_text(data, encoding="utf-8")
        else:
            with log_file.open("a") as fd:
                fd.write(data)

    def log_append(self, key: str, data: str, *, at: Path | str | None = None) -> None:
        """
        Log a message to the repository's log file.

        Args:
            key: The key to log.
            value: The value to log.
        """
        if self._log_file(key, at).exists():
            self.log(key, "\n\n" + data, mode="a")
        else:
            self.log(key, data, mode="w")

    def _log_file(self, key: str, sub_path: Path | str | None = None) -> Path:
        """
        Get the log file path for the given key.
        """
        if sub_path is not None:
            return self._path / sub_path / f".maestro-{key}.log"
        return self._path / f".maestro-{key}.log"

    @contextmanager
    def _write_db(self, *, _save: bool = True):
        file = self._path / ".maestro.yaml"
        try:
            if self._db_cache is not None:
                data = self._db_cache
            elif file.exists():
                source = file.read_text(encoding="utf-8")
                self._db_cache = data = yaml.load(source) or CommentedMap()
                assert isinstance(data, CommentedMap), data
            else:
                self._db_cache = data = CommentedMap()
            yield data
        finally:
            if _save:
                yaml.dump(data, file)

    def _read_only_db(self):
        return self._write_db(_save=False)

    def __delitem__(self, key: K):
        with self._write_db() as db:
            del db[key]

    def __getitem__(self, key: K):
        with self._read_only_db() as db:
            return db[key]

    def __setitem__(self, key: K, value: V):
        with self._write_db() as db:
            db[key] = value

    def __iter__(self) -> Iterator[K]:
        with self._read_only_db() as db:
            return iter(db)

    def __len__(self) -> int:
        with self._read_only_db() as db:
            return len(db)

    if not TYPE_CHECKING:
        update = delegate("update")
        keys = delegate("keys")
        values = delegate("values")
        items = delegate("items")
        setdefault = delegate("setdefault")


class RepoError(Exception):
    """
    Error that occurs during repository processing.
    """

    class Type(enum.Enum):
        """
        Enum for repository error types.
        """

        GENERIC = "generic"
        MISSING_FILES = "missing_files"

    @classmethod
    def missing_files(cls, repo: Repo, files: list[Path]) -> RepoError:
        """
        Create a RepoError for missing files in the repository.
        """
        message = f"Required files not found in repository {repo.path.name}: {', '.join(str(f) for f in files)}"
        return cls(repo, message, type=cls.Type.MISSING_FILES)

    @classmethod
    def from_exception(cls, exc: Exception, repo: Repo | None = None) -> "RepoError":
        """
        Create a RepoError from an exception.
        """
        print(repo)
        print(exc)
        print("trying to create a repo error from an exception")
        raise exc
        return RepoError(
            repo=repo,
            message=str(exc),
            execution=None,
        )

    def __init__(self, repo: Repo, message: str, type=Type.GENERIC, **kwargs):
        self.repo = repo
        self.message = message
        self.type = type
        super().__init__(repo, message, type, kwargs)

        for key, value in kwargs.items():
            setattr(self, key, value)

    def as_failure(self) -> result.Failure[RepoError]:
        """
        Convert this RepoError to a Failure result.
        """
        return result.Failure(self)


#
# Hashing functions
#
def digest(path: Path, cache: bool = False) -> bytes:
    """
    Compute the digest for the current path.

    If cache is True, it tries to use cached values instead of forcing the
    computation.
    """
    if cache:
        return get_hash(path)[0]
    return compute_hash(path, cache=False)


def hexdigest(path: Path, cache: bool = False) -> str:
    """
    Compute the hexdigest for the current path.

    If cache is True, it tries to use cached values instead of forcing the
    computation.
    """
    return digest(path, cache).hex()


def b64digest(path: Path, cache: bool = False) -> str:
    """
    Compute the base64 digest for the current path.

    If cache is True, it tries to use cached values instead of forcing the
    computation.
    """
    if path.is_dir():
        return base64.b64encode(digest(path, cache)).decode("ascii")
    return base64.b64encode(path.read_bytes()).decode("ascii")


def compute_hash(path: Path, cache: bool = True) -> bytes:
    """
    Recompute the hash of a path.
    """

    hashes = {}
    for sub, _, _ in path.walk():
        if sub.is_dir() or sub.name.startswith(".maestro"):
            continue

        with sub.open("rb") as f:
            hash = hashlib.sha256(f.read()).digest()
        key = str(sub.relative_to(path)).encode("utf-8")
        hashes[key] = hash

    combined_hash = hashlib.sha256()
    for key, hash in sorted(hashes.items()):
        combined_hash.update(key)
        combined_hash.update(b":")
        combined_hash.update(hash)
        combined_hash.update(b";")

    hash = combined_hash.digest()
    if cache:
        HASHES[path] = (hash, time.time())
    return hash


def get_hash(path: Path) -> tuple[bytes, float]:
    """
    Get the repository at the given path.

    This function caches the result to avoid repeated disk access.
    """
    if path in HASHES:
        return HASHES[path]

    if len(HASHES) >= MAX_CACHE_SIZE:
        items = sorted(HASHES.items(), key=lambda item: item[1])
        for key, _ in items[:4]:
            del HASHES[key]

    compute_hash(path)
    return HASHES[path]
