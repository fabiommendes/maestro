from pathlib import Path

from unidecode import unidecode
from rich.console import Console
from rich.panel import Panel

from teach.run import lang

from . import models

__all__ = []
export = lambda fn: __all__.append(fn.__name__) or fn
msg = Console()
err = Console(stderr=True)


@export
def compare(
    submission: Path,
    solution: Path,
    ascii: bool = False,
    casefold: bool = False,
    io: Path = None,
    details: bool = False,
):
    """
    Compare submission with correct solution.
    """
    try:
        submission_src = submission.read_text()
        solution_src = solution.read_text()
    except FileNotFoundError as ex:
        msg.print(f"File not found: {ex}")
        exit(1)

    if ascii:
        submission_src = unidecode(submission_src)
        solution_src = unidecode(solution_src)

    if casefold:
        submission_src = submission_src.casefold()
        solution_src = solution_src.casefold()

    if io:
        inputs = (io.parent.joinpath(io.name + ".input")).read_text().split("---")
        outputs = (io.parent.joinpath(io.name + ".output")).read_text().split("---")
        examples = dict(zip(inputs, outputs))
    else:
        examples = None

    code = lang.from_path(solution)
    qst = models.Question.from_source(solution_src, code, tests=examples)
    sub = models.Submission.from_source(submission_src, qst)

    feedback = sub.check()
    feedback.print_rich(msg)

    if details:
        msg.print()
        msg.print("[b green]Details[/]")
        msg.print(f"* [b]Correct?[/b] {yn(feedback.error)}")
        msg.print(f"* [b]Has only valid lines[/b]: {yn(sub.is_permutation())}")
        msg.print(f"* [b]Uses all lines[/b]: {yn(sub.is_full_permutation())}")
        msg.print(f"* [b]Reference answer[/b]: {yn(sub.is_reference_answer())}")
        if io:
            msg.print(f"* [b]Number of tests[/b]: {len(examples)}")


@export
def make_question(
    solution: Path,
    seed: str = None,
):
    """
    Create a new question.
    """
    code = lang.from_path(solution)
    try:
        solution_src = solution.read_text()
    except FileNotFoundError as ex:
        msg.print(f"File not found: {ex}")
        exit(1)

    seed = seed if seed is None else seed.encode("utf-8")
    reference = models.Question.from_source(solution_src, code)
    msg.print(reference.description)

    question = reference.shuffled_lines(seed=seed).simplify_lines(alphabetic=True)
    msg.print(Panel(question.render(), title="Shuffled code"))

    examples = {}
    yes = ("y", "yes", "")
    no = ("n", "no")
    quit = ("q", "quit", "exit")

    msg.input("Press [b green]ENTER[/] to continue...")
    reading = True
    while reading:
        msg.clear()
        msg.print(f"\n[b green]Running example #{len(examples) + 1}[/]")
        input, output = reference.run_example()
        msg.print()

        while True:
            if (resp := msg.input("Use this example? [Y/n/q] ").lower()) in yes:
                examples[input] = output
                break
            elif resp in quit:
                reading = False
                break
            elif resp in no:
                break


@export
def foo():
    pass


def yn(x):
    return "[green]yes[/]" if x else "[red]no[/]"
