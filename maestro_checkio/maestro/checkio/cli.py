from collections import defaultdict
from typing import List
import click
import os
import sys
from pathlib import Path
import json
import pandas as pd
from rich.console import Console
from . import api

console = Console()

CACHED_API_TOKEN = None
TOKEN_CONFIG_PATH = Path.home() / ".config" / "maestro-checkio" / "token"


def get_api_token(token=None):
    """
    Get API token.

    If token is given, return as is, otherwise try to fetch from CHECKIO_API_TOKEN environment
    or from the contents of ~/.config/checkio/token.

    Raise an RuntimeError if no token can be found.
    """
    if token is not None:
        return token
    if CACHED_API_TOKEN is not None:
        return CACHED_API_TOKEN
    if (token := os.environ.get("CHECKIO_API_TOKEN")) is not None:
        set_api_token(token)
        return token
    if TOKEN_CONFIG_PATH.exists():
        token = TOKEN_CONFIG_PATH.read_text().strip()
        set_api_token(token)
        return token
    raise RuntimeError(
        "Could not determine a proper API token.\n\n"
        "You may pass an explicit token string or save the access token in the\n"
        f"CHECKIO_API_TOKEN enviroment variable or save it to {TOKEN_CONFIG_PATH}"
    )


def set_api_token(token: str):
    """
    Configure the global default API token.
    """
    global CACHED_API_TOKEN

    CACHED_API_TOKEN = token


def agg_fn(fn: str):
    if fn.endswith(".json"):
        name = fn.removesuffix(".json")

        with open(fn) as fd:
            data = json.load(fd)

        def fn(x):
            questions = {k for k, v in x.to_dict().items() if v}
            grades = defaultdict(int)
            for question, spec in data.items():
                if question in questions:
                    for k, v in spec.items():
                        grades[k] += v

            return pd.Series(grades)

        fn.__name__ = name

    return fn


@click.group()
@click.option("--token", "-t", help="The API token", default=None)
def cli(token):
    set_api_token(token)


@cli.command()
@click.argument("classroom")
@click.option("--format", "-f", help="The output format")
@click.option("--agg", "-a", help="Reduce by sum, mean, min, max or a json file.")
@click.option("--output", "-o", help="Output file")
@click.option("--token", "-t", help="Chekio token")
def questions(format: str, classroom: str, agg: str, output: Path, token=None):
    """
    Fetch all questions and return a table with the grades for each username.
    """
    df = api.question_dataframe(classroom, token=get_api_token(token))

    # Aggregate data
    if agg:
        if "," in agg:
            agg = [agg_fn(x) for x in agg.split(",")]
        else:
            agg = agg_fn(agg)
        df = df.agg(agg, axis=1).fillna(0)

    # Ouput file or stdout
    if output is None:
        output = sys.stdout
    else:
        if format is None:
            format = output.rpartition(".")[-1]
        output = open(output, "w")

    # Render dataframe with the desired format
    with output:
        if format == "short" or format is None:
            print(df, file=output)
        elif format == "csv":
            df.to_csv(output, mode="w")
        else:
            console.print(f"Invalid format: {format!r}")
            raise SystemExit(1)


@cli.command()
@click.option("--token", "-t", help="Chekio token")
@click.option("--columns", "-c", help="Columns to print")
@click.option("--format", "-f", help="The output format")
@click.option("--output", "-o", help="Output file")
def groups(token: str, columns: List[str], format: str, output: Path):
    """
    Print list of classrooms.
    """
    if isinstance(columns, str):
        columns = [*map(str.strip, columns.split(","))]
    groups = api.groups(get_api_token(token))

    if format == "table":
        console.print(groups.to_dataframe().set_index("slug"))
    elif format is None:
        console.print(groups)
    else:
        raise ValueError("invalid format")


def main():
    cli()
