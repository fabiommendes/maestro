from pathlib import Path
from typing import Optional
from rich.console import Console

from . import lang as _lang
from .code import CodeTransform
from .utils import humanize_time


__all__ = []
export = lambda fn: __all__.append(fn.__name__) or fn
msg = Console()
err = Console(stderr=True)


@export
def echo(path: Path, bracket: str = ""):
    """
    Execute python file echoing inputs whenever the input() function is called.
    """

    def make_input(bracket):
        if bracket is True:
            left, right = "[]"
        elif not bracket:
            left = right = ""
        else:
            left, right = bracket.split(",")

        def echo_input(msg=None):
            out = input(msg)
            print(f"{left}{out}{right}")
            return out

        return echo_input

    src = open(path).read()
    code = compile(src, str(path), "exec")
    eval(code, {"input": make_input(bracket)})


@export
def run(source: Path, lang: str | None = None):
    """
    Run script interactively.
    """
    runner, _ = load_runner_transform(source, lang)
    runner.run_interactive()


@export
def io(
    source: Path,
    input: Path = None,
    output: Path = None,
    lang: str = None,
    single: bool = False,
    echo: bool = False,
    pause: bool = False,
):
    """
    Run script passing input from file.
    """
    if input is None:
        input = source.with_name(source.stem + ".input")

    try:
        input_data = input.read_text()
        if not single:
            examples = input_data.split("---\n")
        else:
            examples = [input_data]
        del input_data

    except FileNotFoundError:
        err.print(f"\n[red b]ERROR![/] Input not found: {input}")
        exit(1)

    if output is not None:
        output = open(output, "w")

    try:
        ctrl, _ = load_runner_transform(source, lang)
        ctrl.prepare()
        if single:
            _show_result(0, examples[0], output)
        else:
            for i, example in enumerate(examples):
                if i != 0 and pause:
                    msg.input("Press \[enter] to continue...")

                msg.print("-" * msg.width, style="yellow")
                msg.print(f"[green b]Example #{i + 1}[/]\n")
                res = ctrl.run(example, echo=output is None and echo and "<>")
                _show_result(i, res, output)
            msg.print("-" * msg.width, style="yellow")
    finally:
        if output is not None:
            output.close()


def _show_result(pos, res, file):
    if file is not None:
        if pos != 0:
            file.write("---\n")
        file.write(res.output)
        if res.error:
            msg.print(f"[red b]🔴 Failed! 🔴[/]")
        else:
            msg.print(f"[b]🚀 Success! ✨[/]")
    else:
        echo = msg.print if res.error is None else err.print
        echo(res.output)
        echo()
    msg.print(f"⏰ Time: {humanize_time(res.time)}", style="b")


@export
def tokenize(source: Path, lang: str = None, verbose: bool = False):
    """
    Tokenize source code.
    """
    transformer = load_code_transform(source, lang)
    tokens = [*transformer.tokens(source.read_text())]
    if verbose:
        for token in tokens:
            msg.print(f"[b]{token.type}[/]: {token.value!r}")
        msg.print()

    msg.print(f"[b green]# tokens[/]: {len(tokens)}")


@export
def metrics(source: Path, lang: str = None):
    """
    Display source code metrics.
    """
    transformer = load_code_transform(source, lang)
    src = source.read_text()
    msg.print("* [b green]loc[/]:", transformer.count_loc(src))
    msg.print("* [b green]lines[/]:", len(src.splitlines()))
    msg.print("* [b green]chars[/]:", len(src))
    msg.print("* [b green]zip[/]:", transformer.count_zip_complexity(src))
    msg.print("* [b green]tokens[/]:", transformer.count_tokens(src))


def load_code_transform(
    path: Path, lang: Optional[str], error=SystemExit(1)
) -> CodeTransform:
    if not path.exists():
        err.print(f"\n[red b]ERROR![/] File not found: {path}")
        raise error
    if lang is None:
        lang = _lang.from_extension(path.suffix)
    return _lang.code_transform(lang)


def load_runner_transform(path: Path, lang: str = None, **kwargs):
    if lang is None:
        lang = _lang.from_extension(path.suffix)
    transform = load_code_transform(path, lang, **kwargs)
    return _lang.path_runner(path, lang), transform
