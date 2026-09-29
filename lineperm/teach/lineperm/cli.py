from pathlib import Path
from typing import Optional

from rich.panel import Panel

from teach.core.cli import out, try_ask

from . import Question, Submission, simplify_lines, randomize_lines
from .util import code_transform


__all__ = ["check", "new"]


def check(
    submission: Path,
    question: Path,
    examples: Path = None,  # type: ignore
):
    """
    Check submission against correct solution.

    Args:
        submission: Path to the submission.
        solution: Path to the submission.
        ascii: Remove non-ascii characters for source files and examples.
        casefold: Ignore case when comparing solutions.
        examples: Path to the IO examples.
    """
    tests = _examples(examples)
    sub, qst = _submission(submission, question, tests)
    feedback = qst.check_submission(sub)
    feedback.print_rich()
    exit(1 if feedback.is_error else 0)


def _submission(submission: Path, solution: Path, tests=None):
    try:
        submission_src = submission.read_text()
        question_src = solution.read_text()
    except FileNotFoundError as ex:
        out.print(f"File not found: {ex}")
        exit(1)

    code = code_transform(solution)
    qst = Question.from_source(question_src, code, tests=tests)
    sub = Submission.from_source(submission_src, qst)

    return sub, qst


def _examples(path: Optional[Path]):
    if path is not None:
        inputs = (path.parent.joinpath(path.name + ".input")).read_text().split("---")
        outputs = (path.parent.joinpath(path.name + ".output")).read_text().split("---")
        examples = dict(zip(inputs, outputs))
    else:
        examples = None
    return examples


def new(
    question: Path,
    seed: str = None,  # type: ignore
    alphabetic: bool = False,
):
    """
    Create a new question.
    """
    code = code_transform(question)
    try:
        qst = Question.from_source(question.read_text(), code)
    except FileNotFoundError as ex:
        out.print(f"File not found: {ex}")
        exit(1)

    bin: bytes | None = None if seed is None else seed.encode("utf-8")
    out.print(qst.header)

    lines = simplify_lines(randomize_lines(qst.lines, seed=bin), alphabetic=alphabetic)
    sub = Submission(qst, lines, qst.header)
    out.print(Panel(sub.render(), title="Shuffled code"))

    out.input("Press [b green]ENTER[/] to continue...")
    examples: dict[str, str] = {}

    while True:
        out.clear()
        out.print(f"\n[b green]Running example #{len(examples) + 1}[/]")
        src, res = qst.run_example()
        out.print()

        match try_ask("Use this example? "):
            case True:
                if src in examples and examples[src] != res:
                    out.print(
                        "[b error]A different example with the same input already exists[/]"
                    )
                    out.print("[b warn]Removing both...[/]")
                    del examples[src]
                elif src in examples and examples[src] == res:
                    out.print("[b info]Example already exists[/]")
                else:
                    examples[src] = res
            case False:
                continue
            case None:
                break

    out.print(examples)
