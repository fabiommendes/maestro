from collections import deque
import io
from numbers import Number
import re
from typing import IO, Any, Callable, Deque, Dict, Iterator, List, Mapping, Tuple, Union

import mistune
from sidekick.functions.signature import Signature
from sidekick.functions import pipeline


from . import models
from .models import CompetencyId, Grades
from .utils import number

PARSE_VALUE = {}
PARSE_KEY = {}

# =============================================================================
# Custom parsers
# =============================================================================
def parse(src: str) -> Deque[dict]:
    """
    Parse markdown string into an AST.
    """
    return deque(markdown_ast.render(src))


def parse_number(str: str) -> Number:
    """
    Read an int of float from string representation.
    """
    try:
        return int(str)
    except ValueError:
        return float(str)


def parse_list(node: dict, values=None, keys=None) -> Dict[str, Any]:
    """
    Parse a Mistune list of items into a dictionary.

    The original document must be of the form::

        * key-1: value-1
        * key-2: value-2

    Args:
        parts:
            A node of type="items
    """
    if typ := node["type"] != "list":
        raise ValueError(f"invalid node type, expect 'list', got, {typ!r}")

    parse_values = _normalize_parser(values)
    parse_keys = _normalize_parser(keys)

    result = {}
    for item in node["body"]:
        parts = item["text"]
        if not parts:
            continue
        if isinstance(parts, str):
            parts = [parts]

        if len(parts) == 1:
            if isinstance(text := parts[0], str):
                parts = deque(map(str.strip, text.split(":", 1)))
            else:
                raise ValueError("not a mapping")
        else:
            parts = deque(parts)

        key = parse_keys(parts.popleft())
        if isinstance(parts[0], str):
            parts[0] = parts[0].lstrip(": ")
            if not parts[0]:
                parts.popleft()
        value = parse_values(parts)
        result[key] = value

    return result


def parse_course(src: str) -> dict:
    """
    Parse a course document.
    """
    md = parse(src)
    name = md.popleft()["text"]
    description = []
    while md[0]["type"] == "paragraph":
        description.append(md.popleft()["text"])

    h2 = md.popleft()
    if h2["type"] != "header" and h2["level"] != 2:
        raise ValueError(f"unexpected node: {h2}")

    student_data = md.popleft()
    if student_data["type"] == "paragraph":
        students = student_data["text"]
    else:
        raise NotImplementedError

    return {"name": name, "description": "\n\n".join(description), "students": students}


def parse_student(src: str) -> models.Student:
    """
    Parse a student document.
    """
    raise NotImplementedError


def parse_competencies(src: str) -> Iterator[models.Competency]:
    """
    Parse a list of competency values from a Markdown document
    representing the competencies of a course.
    """
    for node in parse(src):
        if node["type"] != "list":
            continue
        for key, description in parse_list(node).items():
            is_medal = key.endswith("*")
            yield models.Competency(
                id=key, description=description, is_advanced=is_medal
            )


def parse_exercises(
    src: str, map_id=lambda x: x, parse_values="eval"
) -> Iterator[tuple[CompetencyId, str, Grades]]:
    """
    Parse a document that maps URLs to competency assignments.

    The document structure must contain elements of the form::

        ## [Some title](url)

        * `competency-1`: 1
        * `competency-2`: 2
        * `competency-3`: 3

    This function returns a mapping from each URL to the corresponding mapping
    from competencies to grades.
    """
    out = ("", "", {})
    document = parse(src)
    state = "wait"

    for part in document:
        if state == "wait":
            if part["type"] == "header":
                data = part["text"]
                if not isinstance(data, str) and data["type"] == "link":
                    out = (map_id(data["link"]), data["text"], {})
                    state = "read-data"

        elif state == "read-data":
            if part["type"] == "list":
                data = parse_list(part, values=parse_values)
                data = (_normalize_competency_pair(k, v) for k, v in data.items())
                out[2].update(data)
                yield out

            state = "wait"


def _normalize_competency_pair(k: str, v: Union[str, Number]) -> Tuple[str, Number]:
    if isinstance(v, Number):
        return (k, v)
    elif isinstance(v, str):
        if m := re.match("^\d+(\.\d+)?", v.strip()):
            return (k, number(m.group(0)))
        raise ValueError(f"invalid numeric representation: {v!r}")
    elif v is None:
        return 0
    raise TypeError(type(v))


def render_node(node: dict[str, Any] | str) -> str:
    """
    Renders a parsed Mistune node.
    """
    fd = io.StringIO()
    dump_node(node, fd)
    return fd.getvalue()


def dump_node(node: dict[str, Any] | str, file: IO):
    """
    Renders a parsed Mistune node to file.
    """
    if isinstance(node, str):
        file.write(node)

    elif isinstance(node, (list, tuple, deque)):
        for part in node:
            dump_node(part, file)

    elif isinstance(node, dict):
        match node["type"]:
            case "list":
                for item in node["body"]:
                    file.write("* ")
                    dump_node(item, file)
                    file.write("\n")
            case "paragraph":
                dump_node(node["text"], file)
                file.write("\n\n")
            case _:
                raise NotImplementedError

    else:
        raise TypeError(type(node))


# =============================================================================
# Auxiliary classes and methods
# =============================================================================
class MistuneAST:
    """
    Renders document as an AST.
    """

    def __init__(self, **kwargs):
        self.options = kwargs

    def __getattr__(self, attr):
        method = getattr(mistune.Renderer, attr)
        sig = Signature.from_callable(method)
        argnames = [*sig.parameters.keys()]
        del argnames[0]  # self

        def fn(self, *args, **kwargs):
            bind = sig.bind(self, *args, **kwargs)
            data = {
                "type": attr,
                **dict(zip(argnames, args)),
                **bind.kwargs,
            }

            text = data.get("text")
            if isinstance(text, list) and len(text) == 1:
                data["text"] = text[0]
            return [data]

        fn.__name__ = attr
        setattr(type(self), attr, fn)
        return getattr(self, attr)

    def placeholder(self):
        return []

    def text(self, text):
        return [text]


markdown_ast = mistune.Markdown(renderer=MistuneAST())


def _normalize_parser(spec) -> Callable[[dict], Any]:
    if spec in (None, "text"):
        return _clean_parser
    elif spec == "eval":
        return _eval_parser
    elif callable(spec):
        return spec
    elif isinstance(spec, (list, tuple)):
        return pipeline(*map(_normalize_parser, spec))
    else:
        raise ValueError(f"cannot be interpreted as a parser: {spec}")


def _clean_parser(node) -> str:
    if isinstance(node, str):
        return node
    elif isinstance(node, Mapping):
        return _clean_parser(node["text"])
    return "".join(_clean_parser(x) for x in node)


def _eval_parser(node):
    src = _clean_parser(node)
    for func in [int, float, complex]:
        try:
            return func(src)
        except ValueError:
            continue
    return src
