import os
from pathlib import Path

import click

import ezio
from .commands import ExtractAuthorsFromZipFile, LoadFile, CheckAuthors


@click.group()
def cli():
    pass


# ==============================================================================
# maestro nb <command>
# ==============================================================================
@cli.group()
def nb():
    pass


@nb.command()
@click.argument('filename')
@click.option(
    '--path', '-p', default='.',
    help='Base path in which nbgrader stores submissions.',
)
@click.option(
    '--name', '-n', default=None,
    help='Name of activity (e.g., exam-1)',
)
@click.option(
    '--category', '-c', default=None,
    help='Name of category (e.g., exams)',
)
def load(filename, path, name, category):
    """
    Extract user names from values.
    """
    from .nb import load_zip

    if category is None:
        category = os.path.splitext(os.path.basename(filename))[0]
    if name is None:
        name = os.path.splitext(os.path.basename(filename))[0]

    load_zip(Path(filename), Path(path), name, category)


@nb.command()
@click.option(
    '--path', '-p', default='./gradebook.db',
    help='Base path to the gradebook.db database.',
)
@click.option(
    '--output', '-o', default=None,
    help='Output file.',
)
@click.option(
    '--simple/--full', default=False,
    help='Display only minimal information.',
)
@click.option(
    '--normalize', '-n', type=float, default=None,
    help='Normalize all grades relative to the maximum grade.',
)
@click.option(
    '--sort', '-s', default='id',
    help='Sort according to column',
)
def export(path, output, simple, normalize, sort):
    """
    Export grading data as comma separated values.
    """
    from ..classroom_db.nb_gradebook import NbGradebook

    gb = NbGradebook(path)
    df = gb.gradebook(full=not simple, normalized=normalize is not None)
    if normalize:
        n = 2 if not simple else 0
        df[df.columns[n:]] *= normalize

    if sort == 'id':
        df = df.sort_index()
    elif sort in df.columns:
        df = df.sort_values(sort)
    else:
        msg = f'<b><red>ERROR!</red></b> Invalid column to sort: <b>{sort}</b>'
        cols = ', '.join(df.columns)
        ezio.print(msg, format=True)
        ezio.print(f'Columns must be one of {cols}, or id')
        raise SystemExit()

    # Write
    if output:
        df.to_csv(output, float_format='%.2f')
    else:
        print(df.to_csv(float_format='%.2f'))


# ==============================================================================
# Main
# ==============================================================================

def main():
    return cli()


if __name__ == '__main__':
    main()
