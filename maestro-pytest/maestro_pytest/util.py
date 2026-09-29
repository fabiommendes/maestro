from functools import partial

from slugify import slugify
from toml import dumps


def toml_escape(st: str) -> str:
    """
    Escape a string for safe inclusion in a TOML file.
    """
    return dumps({"key": st})[6:-1]


slugify = partial(
    slugify,
    max_length=50,
    entities=False,
    word_boundary=True,
    allow_unicode=True,
)
