from collections import namedtuple

import re
from hyperpython import table, tr, td, th, div

Ex = namedtuple('Example', ['rows', 'cols'])


def check_regex_crossword(answer, rows, cols, silent=False):
    r"""
    A regex crossword is defined by a list of rows and a list of columns
    containing regular expression strings. A valid answer is a rectangular
    table of shape [n, m] (where `n = len(rows)` and `m = len(cols)`) in
    which every string formed by concatenating elements in a row must match
    the corresponding column regex and likewise for rows.


    Example:

    >>> ans = [
    ...   ["1", "2"],
    ...   ["3", "4"]
    ... ]
    >>> check_regex_crossword(ans, cols=[r"12|34", r".\d"], rows=[r"13|24", r"[123][a4]"])
    Congratulations!
    """
    if isinstance(answer, str):
        answer = answer.strip('\n').splitlines()

    for i, expr in enumerate(rows):
        row = ''.join(answer[i])
        if not re.fullmatch(expr, row):
            raise AssertionError(f'Does not match row {i + 1} /{expr}/: {row!r}')

    for i, expr in enumerate(cols):
        col = ''.join(line[i] for line in answer)
        if not re.fullmatch(expr, col):
            raise AssertionError(f'Does not match column {i + 1} /{expr}/: {col!r}')

    if not silent:
        print('Congratulations!')


def regex_crossword_table(rows, cols):
    """
    Render a regular expression crossword table from its defining rows and
    columns.
    """
    head_style = (
        "transform: rotate(-90deg) translate(0, 1rem); "
        "transform-origin: 0% 0%; "
        "height: 1.5rem; "
        "width: 12rem; "
        "text-align: left; "
        "position: absolute; "
        "font-size: 1.5rem"
    )
    td_style = (
        "width: 4rem; "
        "height: 4rem; "
        "border: 1px solid black;"
    )
    table_style = (
        "transform: rotate(30deg); "
        "font-family: monospace; "
        "margin: auto"
    )

    return div(style="margin: 3rem auto; padding-right: 5rem;")[
        table(style=table_style)[[
            # Head
            tr(style="height: 12rem; background: none")[[
                th(),
                *[th(div(r, style=head_style), style="vertical-align: bottom")
                  for r in cols]
            ]],

            # Body rows
            *(tr([
                th(r, style="font-size: 1.5rem"),
                *(td(" ", style=td_style) for _ in cols)
            ]) for r in rows)
        ]]
    ]


#
# Library of examples (safe to use since it does not contain answers).
#
CROSSWORD_EXAMPLES = {
    # Initial examples
    "abba": Ex(
        rows=["ab", "cd"],
        cols=["ac", "db"],
    ),
    "abc?": Ex(
        rows=["ab?c?", "c?d?e?"],
        cols=["a+", "ca*d*"],
    ),
    # Test, 10/2019
    "john": Ex(
        rows=["[AWE]+", "[ALP]+K", "(PR|ER|EP)"],
        cols=["[BQW](PR|LE)", "[RANK]+"],
    ),
    "yr": Ex(
        rows=[r"18|19|20", r"[6789]\d"],
        cols=[r"\d[2480]", r"56|94|73"],
    ),
}
