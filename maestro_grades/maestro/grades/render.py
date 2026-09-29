from functools import singledispatch
import jinja2

from . import models

jinja_loader = jinja2.PackageLoader("maestro.grades")
jinja_env = jinja2.Environment()


def render_template(template: str, env: dict) -> str:
    """
    Render template for the given environment variables.
    """
    jinja = jinja_loader.load(jinja_env, template)
    return jinja.render(env)


@singledispatch
def markdown(obj):
    """
    Render object as markdown.
    """
    return str(obj)


@markdown.register(models.Course)
def _(obj) -> str:
    return render_template("course.jinja", {"course": obj})


@markdown.register(models.Student)
def _(obj) -> str:
    lines = [
        f"* Nome: {obj.name}",
        f"* Matrícula: {obj.id}",
    ]
    for acc, label in obj.EXTERNAL_ACCOUNTS.items():
        if username := getattr(obj, acc):
            lines.append(f"* {label}: {username}")
    return "\n".join(lines)
