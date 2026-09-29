from abc import ABC, abstractproperty
from typing import Sequence, TypeVar, Protocol
from dataclasses import dataclass, field


from teach.core import Feedback
from teach.core.error import Error, TimeoutError, RuntimeError, CompilationError

from .code import Lang

Src = TypeVar("Src", contravariant=True)


class BoundRunner(Protocol):
    """
    Runner that is bound to some specific program or interaction.
    """

    def run_io(self, input: str) -> "Execution":
        """
        Run code, passing the string input.

        The execution captures a string with the printed output.
        """
        return self.run_echo(input, ("", ""))

    def run_echo(self, input: str, braces: str | tuple[str, str] = "[]") -> "Execution":
        """
        Run code, passing the string input.

        Return a string with the printed output.
        """
        raise NotImplementedError

    def run_interactive(self) -> "Execution":
        """
        Run code in interactive mode.

        Inputs and outputs are not captured and are usually empty in the resulting
        execution object.
        """
        return self.run_capture()

    def run_capture(self) -> "Execution":
        """
        Run code in interactive mode, but capture both input and output.
        """
        raise NotImplementedError

    def run_checks(self, input: str, *args, **kwargs) -> "Feedback":
        """
        Run code, passing input, then check the output against the expected value.

        Return a feedback object with the results from interaction.
        """
        execution = self.run_io(input)
        return execution.check_output(*args, **kwargs)


class BoundRunnerDelegate(BoundRunner, ABC):
    """
    Concrete implementation that delegates implementations to the `_bound_runner`
    attribute.

    The attribute can be created during intialization as a property to avoid
    caching de runner between executions.
    """

    @abstractproperty
    def _runner_instance(self) -> "BoundRunner":
        """
        Return a bound runner.
        """
        raise NotImplementedError

    def run_io(self, input: str) -> "Execution":
        return self._runner_instance.run_io(input)

    def run_echo(self, input: str, braces: str | tuple[str, str] = "[]") -> "Execution":
        return self._runner_instance.run_echo(input, braces)

    def run_interactive(self) -> "Execution":
        return self._runner_instance.run_interactive()

    def run_capture(self) -> "Execution":
        return self._runner_instance.run_capture()

    def run_checks(self, input: str, *args, **kwargs) -> "Feedback":
        return self._runner_instance.run_checks(input, *args, **kwargs)


class SrcRunner(Protocol[Src]):
    """
    A runner that requires source code as input.
    """

    def bind(self, src: Src, lang: Lang) -> BoundRunner:
        """
        Return a bound runner for the given source and language.
        """
        raise NotImplementedError

    def run_io(self, src: Src, lang: Lang, input: str) -> "Execution":
        """
        Run code, passing the string input.

        The execution captures a string with the printed output.
        """
        return self.bind(src, lang).run_io(input)

    def run_echo(
        self, src: Src, lang: Lang, input: str, braces: Sequence[str] = "[]"
    ) -> "Execution":
        """
        Run code, passing the string input.

        Return a string with the printed output.
        """
        return self.bind(src, lang).run_echo(input)

    def run_interactive(self, src: Src, lang: Lang) -> "Execution":
        """
        Run code in interactive mode.

        Inputs and outputs are not captured and are usually empty in the resulting
        execution object.
        """
        return self.bind(src, lang).run_interactive()

    def run_capture(self, src: Src, lang: Lang) -> "Execution":
        """
        Run code in interactive mode, but capture both input and output.
        """
        return self.bind(src, lang).run_capture()

    def run_checks(
        self, src: Src, lang: Lang, input: str, *args, **kwargs
    ) -> "Feedback":
        """
        Run code, passing input, then check the output against the expected value.

        Return a feedback object with the results from interaction.
        """
        return self.bind(src, lang).run_checks(input, *args, **kwargs)


@dataclass(frozen=True)
class Execution:
    """
    The result of running a code.
    """

    input: str
    output: str
    time: float
    errors: list[Error] = field(default_factory=list)

    @property
    def is_success(self) -> bool:
        return not self.errors

    @property
    def is_error(self) -> bool:
        return bool(self.errors)

    @classmethod
    def timeout_error(cls, input: str, timeout: float, **kwargs) -> "Execution":
        """
        Create an execution after a timeout error.
        """
        kwargs.setdefault("errors", []).append(TimeoutError(timeout))
        kwargs.setdefault("time", timeout)
        kwargs.setdefault("output", "")
        return cls(input, **kwargs)

    @classmethod
    def runtime_error(cls, input: str, code=1, **kwargs) -> "Execution":
        """
        Create an execution after a runtime error.
        """
        kwargs.setdefault("errors", []).append(RuntimeError(code))
        kwargs.setdefault("output", "")
        return cls(input, **kwargs)

    @classmethod
    def compilation_error(cls, msg: str, **kwargs) -> "Execution":
        """
        Create an execution after a compilation error.
        """
        kwargs.setdefault("errors", []).append(CompilationError(msg))
        kwargs.setdefault("output", "")
        kwargs.setdefault("input", "")
        return cls(**kwargs)

    def valid_output(self, exception_cls=ValueError) -> str:
        """
        Return the output string if the execution was successful.
        """
        if self.is_success:
            return self.output
        raise exception_cls(self.errors)

    def feedback(self) -> Feedback:
        """
        Return the feedback for the execution.
        """
        return Feedback.Multi([Feedback.Error(e) for e in self.errors])

    def check_output(self, *args, **kwargs) -> Feedback:
        """
        Check the output of the execution.
        """
        if self.is_success:
            # TODO: checker = StringCheck(*args, **kwargs)
            raise NotImplementedError
        return self.feedback()
