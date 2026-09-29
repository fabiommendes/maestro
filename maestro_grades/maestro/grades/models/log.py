import json
from dataclasses import dataclass
from typing import Any, Callable, Iterator
from pathlib import Path


from .base import Model

__all__ = ["JSONLog"]


@dataclass
class JSONLog(Model):
    """
    Log entries in a given file.

    All operations are atomic and the log file is open and closed
    at each read and write operation. The log is stored as a newline
    separated JSON.
    """

    path: Path

    def __iter__(self) -> Iterator[dict]:
        return self.iter_entries()

    def _get_data(self):
        return list(self.iter_entries())

    def _set_data(self, data):
        with open(self.path, "w") as fd:
            for entry in data:
                json.dump(entry, fd)
                fd.write("\n")

    def _append_data(self, entry):
        with open(self.path, "a+") as fd:
            match fd.tell():
                case 0:
                    pass
                case n:
                    fd.seek(n - 1)
                    if fd.read(1) != "\n":
                        fd.write("\n")
            fd.write(json.dumps(entry))
            fd.write("\n")

    def iter_entries(self) -> Iterator[dict]:
        """
        Iterate over all log entries.
        """
        with open(self.path) as fd:
            for line in fd:
                if line.strip():
                    if line.startswith("//"):
                        continue
                    yield json.loads(line)

    def has_entry(self, pred: Callable[[Any], bool]) -> bool:
        """
        Return True if some entry accepts the given predicate.
        """
        return any(map(pred, self.iter_entries()))

    def add_entry(self, entry: Any):
        """
        Add log entry.
        """
        self._append_data(entry)
