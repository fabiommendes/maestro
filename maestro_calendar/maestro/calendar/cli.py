import click
from .calendar import parse


@click.command("maestro.calendar")
@click.argument("source", type=click.File("r"))
@click.option("--output", "-o", default=None, type=click.File("w"), help="Output file")
@click.option(
    "--info",
    "-i",
    is_flag=True,
    help="Only prints basic information about file",
)
def main(source, output, info):
    """
    Renders a calendar from the given markdown source file.
    """

    cal = parse(source.read())
    if info:
        click.echo(cal.describe())
    elif output:
        output.write(cal.render_rst())
    else:
        click.echo(cal)
