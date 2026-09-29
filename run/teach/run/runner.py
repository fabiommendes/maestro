import select
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import (
    IO,
    Dict,
    Iterator,
    List,
    Any,
    Optional,
    Sequence,
    Union,
    Tuple,
    TYPE_CHECKING,
    cast,
)

from sidekick.types import Union as UnionType, Case

from teach.core import Feedback

from .utils import humanize_time
from .base import BoundRunner, Execution


if TYPE_CHECKING:
    from enum import Enum

    class ellipsis(Enum):
        Ellipsis = "..."

    Ellipsis = ellipsis.Ellipsis
else:
    ellipsis = type(Ellipsis)


class Runner(BoundRunner):
    """
    Script runner class.
    """

    timeout: Optional[float] = None
    is_closed: bool = False
    resources: List[Any] = field(default_factory=list)

    def __del__(self) -> None:
        self.close()

    def prepare(self) -> Feedback:
        """
        Prepare the runner.

        This step is used to compile and load resources, when necessary.
        """
        return Feedback.Success()

    def close(self):
        """
        Clean all resources and close the runner.
        """
        if self.is_closed:
            return

        resources = getattr(self, "resources", [])
        while resources:
            resource = resources.pop()

            try:
                resource.close()
                continue
            except Exception:
                pass

            try:
                resource.__exit__()
            except Exception:
                pass
        self.is_closed = True

    def _check_closed(self, msg="Runner is closed!"):
        if self.is_closed:
            raise RuntimeError(msg)

    def add_resource(self, resource: Any) -> None:
        """
        Append resource to runner.

        Resources are cleaned when the runner is destroyed or when the
        close() method is called.
        """
        self._check_closed("cannot add resources to a closed runner.")
        self.resources.append(resource)


@dataclass
class ScriptRunner(Runner):
    """
    Run scripts.
    """

    cmd: List[str]
    cwd: Optional[Path] = None
    timeout: Optional[float] = None
    resources: List[Any] = field(default_factory=list)
    env: Dict[str, str] = field(default_factory=dict)
    is_closed: bool = False

    def run_io(self, input: str, echo: bool = False) -> Execution:
        kwargs = {"text": True, "timeout": self.timeout, "capture_output": True}
        return self._run(False, input=input, **kwargs)

    def run_echo(self, input: str, braces: str | tuple[str, str] = "[]") -> Execution:
        kwargs = {"text": True, "timeout": self.timeout, "capture_output": True}
        return self._run(True, input=input, braces=braces, **kwargs)

    def run_interactive(self) -> Execution:
        return self._run(False, text=True)

    def run_capture(self) -> Execution:
        # Inner actions
        def register_output(line):
            sys.stdout.write(line)
            stdout.append(line)

        def read_in_background(fd: IO):
            while not fd.closed:
                try:
                    ch = fd.read(1)
                except ValueError:
                    return
                if ch:
                    register_output(ch)
                if ch == "\n":
                    sys.stdout.flush()

        # Initialize processes
        stdin: list[str] = []
        stdout: list[str] = []
        t0 = time.monotonic()
        proc = subprocess.Popen(
            **self._prepare_popen_args(
                echo=False,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
        )
        reader_bg = threading.Thread(target=read_in_background, args=(proc.stdout,))
        reader_bg.start()

        # The main loop reads from input while proc.stdin is ready to
        # receive input
        while True:
            if proc.poll() is not None:
                break

            time.sleep(0.05)
            is_waiting_input = select.select([], [proc.stdin], [])[1]
            if is_waiting_input and proc.poll() is None:
                data = input() + "\n"
                stdin.append(data)
                try:
                    if proc.stdin is not None:
                        proc.stdin.write(data)
                        proc.stdin.flush()
                except ValueError:
                    break

        register_output(proc.communicate()[0])
        try:
            reader_bg.join(timeout=1.0)
        except Exception:
            pass
        dt = time.monotonic() - t0
        return Execution(input="".join(stdin), output="".join(stdout), time=dt)

    def _run(self, echo=False, **kwargs):
        t0 = time.monotonic()
        popen_args = self._prepare_popen_args(echo, **kwargs)
        inputs = popen_args.get("input", "")
        try:
            proc = subprocess.run(**popen_args)
        except subprocess.TimeoutExpired as exc:
            dt = time.monotonic() - t0
            output = ((exc.stdout or b"") + (exc.stderr or b"")).decode("utf-8")
            return Execution.timeout_error(inputs, dt, output=output)

        dt = time.monotonic() - t0
        output = proc.stdout + proc.stderr
        if proc.returncode != 0:
            code = proc.returncode
            return Execution.runtime_error(inputs, code, output=output, time=dt)
        return Execution(inputs, output, time=dt)

    def _prepare_cmd(self, echo: bool) -> List[str]:
        return self.cmd[:]

    def _prepare_popen_args(self, echo: bool, **kwargs) -> Dict[str, Any]:
        self._check_closed("cannot execute a closed runner.")

        kwargs.setdefault("args", self._prepare_cmd(echo))
        kwargs.setdefault("encoding", "utf-8")
        if self.cwd:
            kwargs.setdefault("cwd", self.cwd)
        if self.env:
            kwargs.setdefault("env", self.env)
        return kwargs


@dataclass
class PythonScriptRunner(ScriptRunner):
    """
    Run Python scripts.
    """

    @property
    def path(self) -> Path:
        return Path(self.cmd[-1])

    def __init__(self, path: Path, **kwargs) -> None:
        if "cmd" in kwargs:
            raise TypeError('invalid keyword argument: "cmd"')
        path = path.resolve()
        python = str(kwargs.pop("python", sys.executable))
        kwargs.setdefault("cwd", path.parent)
        super().__init__(cmd=[python, "-X", "utf8", str(path)], **kwargs)

    def _prepare_cmd(self, echo: bool) -> List[str]:
        cmd = super()._prepare_cmd(echo)
        if echo:
            cmd.extend(["-m", "teach.run", "echo", cmd.pop()])
        return cmd

    def _prepare_popen_args(self, echo: bool, **kwargs) -> Dict[str, Any]:
        return super()._prepare_popen_args(echo, **kwargs)

    def prepare(self) -> Feedback:
        src = self.path.read_text()
        try:
            compile(src, self.path, "exec")
        except SyntaxError as exc:
            return Feedback.Fail("syntax-error", payload=exc)
        return Feedback.Success()


@dataclass
class SrcScriptRunner(Runner):
    """
    Run script from source in a temporary directory.
    """

    source: str
    name: str
    cmd: List[Union[str, ellipsis]]
    timeout: Optional[float] = None
    env: Dict[str, str] = field(default_factory=dict)
    files: Dict[Path, Union[str, bytes]] = field(default_factory=dict)
    resources: List[Any] = field(default_factory=list)
    is_closed: bool = False
    encoding: str = "utf-8"

    @contextmanager
    def _runner(self, echo: bool) -> Iterator[ScriptRunner]:
        self._check_closed("cannot execute a closed runner.")

        with TemporaryDirectory() as _root:
            root = Path(_root)
            self._make_files(root)
            yield self._prepare_script_runner(root, echo)

    def _prepare_script_runner(self, root: Path, echo: bool) -> ScriptRunner:
        cmd = self._prepare_cmd(root, echo)
        return ScriptRunner(cmd, cwd=root, timeout=self.timeout, env=self.env)

    def _make_files(self, root: Path):
        path = root.joinpath(self.name).resolve()
        path.write_text(self.source, encoding=self.encoding)

        for name, data in self.files.items():
            path = root.joinpath(name).resolve()
            path.parent.mkdir(parents=True, exist_ok=True)

            if isinstance(data, str):
                data = data.encode("utf-8")
            path.write_bytes(data)

    def _prepare_cmd(self, root: Path, echo: bool) -> List[str]:
        name = str(root.joinpath(self.name))
        return [(name if x is ... else x) for x in self.cmd]  # type: ignore

    def run_io(self, input: str, echo: bool = False) -> Execution:
        with self._runner(echo) as runner:
            return runner.run_io(input)

    def run_interactive(self) -> Execution:
        with self._runner(echo=False) as runner:
            return runner.run_interactive()

    def run_capture(self) -> Execution:
        with self._runner(echo=False) as runner:
            return runner.run_capture()


class PythonSrcScriptRunner(SrcScriptRunner):
    """
    Run Python code from source.

    The script runner respects the "echo" parameter by patching the
    input() function in the interpreter.
    """

    def __init__(self, source: str, name: str = "main.py", **kwargs) -> None:
        if "cmd" in kwargs:
            raise TypeError('invalid keyword argument: "cmd"')
        python = str(kwargs.pop("python", sys.executable))
        cmd = [python, "-X", "utf8", cast(str, ...)]
        super().__init__(source, name=name, cmd=list(cmd), **kwargs)

    def _prepare_cmd(self, root, echo: bool) -> List[str]:
        cmds = super()._prepare_cmd(root, echo)
        if echo:
            *cmds, name = cmds
            cmds.extend(["-m", "teach.run", "echo", name])
        return cmds

    def _make_files(self, root: Path):
        path = root.joinpath(self.name).resolve()
        path.write_text("#-*- coding: utf8 -*-\n" + self.source, encoding=self.encoding)

        for name, data in self.files.items():
            path = root.joinpath(name).resolve()
            path.parent.mkdir(parents=True, exist_ok=True)

            if isinstance(data, str):
                data = data.encode("utf-8")
            path.write_bytes(data)

    _prepare_popen_args = PythonScriptRunner._prepare_popen_args
    prepare = PythonScriptRunner.prepare  # type: ignore
