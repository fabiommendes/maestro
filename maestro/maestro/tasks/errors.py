from __future__ import annotations

from io import StringIO
from subprocess import CalledProcessError

from ..repo import Repo, RepoError


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
        repo: Repo | None = None,
    ):
        from .subprocess import Execution

        if isinstance(stdout, StringIO):
            stdout = stdout.getvalue()

        self.execution = Execution(cmd, code, StringIO(stdout))
        self.repo = repo
        super().__init__(code, cmd, output=stdout)

    def _to_repo_error_(self, repo: Repo | None = None) -> RepoError:
        repo = self.repo if repo is not None else repo
        if repo is None:
            raise ValueError("Repository must be provided to create RepoError")

        return RepoError(
            repo=repo,
            message=str(self),
            execution=self.execution,
        )
