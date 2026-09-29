from __future__ import annotations

import io
import sys
from contextlib import redirect_stdout

import rich


class UserFacingError(Exception):
    """
    Base class for user-facing errors.
    """

    ERROR_CODE: int = 1

    @classmethod
    def from_error(
        cls, error: str | Exception, error_code: int | None = None
    ) -> UserFacingError:
        """
        Wraps the error with a new message.
        """
        return UserFacingError(error, error_code=error_code)

    def __init__(self, value: str | Exception, error_code: int | None = None):
        """
        Initialize the UserFacingError with a message and an error code.
        """
        if error_code is None:
            error_code = self.ERROR_CODE

        super().__init__(value)
        self.error_code = error_code
        self.value = value

    def __str__(self):
        with redirect_stdout(io.StringIO()) as fd:
            self.print()
            return fd.getvalue()

    def print(self, stderr: bool = False) -> None:
        """
        Print the error message to the console.
        """

        kwargs = {"file": sys.stderr} if stderr else {}
        rich.print(f"[b red]Error:[/] {str(self.value)}", **kwargs)  # type: ignore

    def panic(self):
        """
        Print the error message and exit the program.
        """
        self.print()
        raise self


class ResultError(UserFacingError):
    """
    Raised when computation produced some invalid result.
    """


class ConfigurationError(Exception):
    """
    Raised when there is an error in the configuration.

    Usually this is related
    """


class SubprocessError(UserFacingError):
    """
    Raised when there is an error in a subprocess.

    This is usually related to a command that failed to execute.
    """

    def __init__(self, value: str | Exception, error_code: int = 1):
        """
        Initialize the SubprocessError with a message and an error code.
        """
        super().__init__(value, error_code)
        self.value = value
