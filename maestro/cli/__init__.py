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
# maestro git <command>
# ==============================================================================
@cli.group()
def git():
    pass


@git.command()
@click.argument('source')
@click.option(
    '--repo', '-r', default=None,
    help='Name repository URL column',
)
@click.option(
    '--path', '-p', default=None,
    help='Column with the output path',
)
@click.option(
    '--branch', '-b', default=None,
    help='Column with branch name',
)
@click.option(
    '--default-branch', default='master',
    help='Default branch, if not given',
)
@click.option(
    '--depth', '-d', default=None,
    help='Branch depth',
)
def clone(source, repo, path, branch, default_branch, depth):
    """
    Clone all given git repositories.
    """
    kwargs = locals()
    source = kwargs.pop('source')

    from .git import clone_repos_at as do
    do(source, **kwargs)


# ==============================================================================
# Shell commands
# ==============================================================================
@cli.command()
@click.argument('source')
@click.argument('dest')
@click.option(
    '--force', '-f', is_flag=True,
    help='Force copy of duplicate files',
)
def cp(source, dest, force):
    from subprocess import run
    import os

    def do(cmd):
        if force:
            cmd = [*cmd, '-f']
        print(' '.join(cmd))
        run(cmd)

    dest = dest.lstrip('.')
    source = os.path.abspath(source)
    for p in os.listdir(os.getcwd()):
        if not os.path.isdir(p):
            continue
        subdir = os.path.abspath(p)
        cmd = ['cp', source, os.path.join(subdir, dest), '-r']
        do(cmd)


@cli.command()
def clean():
    for base, dirs, files in os.walk('.'):
        if base.endswith(os.path.sep + '__pycache__' + os.path.sep):
            for file in files:
                os.unlink(os.path.join(base, file))
            os.rmdir(base)


@cli.command()
@click.option(
    '--answers', '-a', default='answers.json',
    help='Path to answers file',
)
@click.option(
    '--rerun', '-r', is_flag=True,
    help='Force Pytest re-run',
)
def test(answers, rerun):
    import json
    import subprocess

    # Load answers file
    if os.path.exists(answers):
        answers = json.load(open(answers))
    else:
        answers = None

    # Collect results
    results = {}
    fname = 'test-results.json'
    for dir in os.listdir(os.getcwd()):
        if not os.path.isdir(dir):
            continue

        if rerun or not os.path.exists(os.path.join(dir, fname)):
            env = {'PYTHONPATH': '.'}
            subprocess.run(['pytest', '--json-report', '--json-report-file', fname],
                           cwd=dir, env=env)

        with open(os.path.join(dir, fname)) as fd:
            data = json.load(fd)
            data = data['tests']
            data = {obj['nodeid']: obj['outcome'] == 'passed' for obj in data}
            results[dir] = data

    # Get answer grader
    tests = set()
    for obj in results.values():
        tests.update(obj)

    for k, v in results.items():
        keys = set(v)
        if tests - keys:
            lst = '\n  * '.join(tests - keys)
            print(f'Missing tests [{k}]:\n  * {lst}')

    # Answer file
    if answers is None:
        data = {k: 1 for k in sorted(tests)}
        print(json.dumps(data, indent=4))
        return

    # Save grades
    import pandas as pd
    grades = []
    total = sum(answers.values())
    for gid, tests in results.items():
        grade = sum(answers[tid] for tid, is_ok in tests.items() if is_ok)
        grades.append([gid, 100 * grade / total])
    df = pd.DataFrame(grades, columns=['group', 'grade'])
    df.to_csv('grades.csv')


@cli.command()
@click.argument('cmd')
def run(cmd):
    import subprocess

    for dir in dirs():
        subprocess.run(cmd, shell=True, cwd=dir)


def dirs(path=None):
    path = path or os.getcwd()
    for dir in os.listdir(path):
        if not os.path.isdir(dir):
            continue
        yield dir


# ==============================================================================
# Tools
# ==============================================================================
@cli.command()
@click.argument('source', type=click.File('r'))
@click.option(
    '--output', '-o', default=None,
    type=click.File('w'), help='Output file')
@click.option(
    '--info', '-i', is_flag=True,
    help='Only prints basic information about file',
)
def calendar(source, output, info):
    from ..tools import calendar

    cal = calendar.parse(source.read())
    if info:
        click.echo(cal.describe())
    elif output:
        output.write(str(cal))
    else:
        click.echo(cal)


# ==============================================================================
# Main
# ==============================================================================
def main():
    return cli()


if __name__ == '__main__':
    main()
