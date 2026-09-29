from . import cli
from sidekick.app.cli import run


def main():
    """
    Grade exercises based on organizing permutation of lines to find a proper solution.
    """
    cli.__name__ = "teach.lineperm"
    cli.__doc__ = main.__doc__
    run(cli)


if __name__ == "__main__":
    main()
