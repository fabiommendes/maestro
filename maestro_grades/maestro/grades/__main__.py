from . import cli
from sidekick.app.cli import run


def main():
    """
    A group of grading related methods.
    """
    cli.__name__ = "maestro.grades"
    cli.__doc__ = main.__doc__
    run(cli)


if __name__ == "__main__":
    main()
