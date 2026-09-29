from __future__ import annotations

from io import StringIO
from pathlib import Path
from subprocess import CalledProcessError

# from ..repo import Repo, RepoError


class ExecutionError(CalledProcessError):
    """
    Represents an error that occurred during the execution of an external
    command.

    Attributes:
        cmd:
            The command that was executed.
        code:
            The exit code of the command.
        stdout:
            The output of the command.
    """

    def __init__(
        self,
        cmd: str | list[str],
        code: int,
        stdout: str | StringIO = "",
        cwd: Path | None = None,
    ):
        from .subprocess import Execution

        if isinstance(stdout, StringIO):
            stdout = stdout.getvalue()

        self.execution = Execution(cmd, code, StringIO(stdout))
        self.cwd = cwd
        super().__init__(code, cmd, output=stdout)
