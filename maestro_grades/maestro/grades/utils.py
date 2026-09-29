from functools import singledispatch
from numbers import Number
from typing import Any, Iterable, Callable, Optional, Pattern
import re

from . import config

# Generic regular expressions
URL_START = re.compile(r"https?://")

# User URLs
CHEKIO_USER_URL = re.compile(
    r"(?:\w+\.)?checkio\.org/user/(?P<user>[^\/]+)\/?",
)
GITHUB_USER_URL = re.compile(
    r"github\.com/(?P<user>[^\/]+)\/?",
)
BEECROWD_USER_URL = re.compile(
    r"(?:www\.)?beecrowd\.com\.br/judge/(?:[\w_-]+)/profile/(?P<user>\d+)\/?",
)
USER_URLS = [CHEKIO_USER_URL, GITHUB_USER_URL, BEECROWD_USER_URL]

# Exercises
CHEKIO_EXERCISE_URL = re.compile(
    r"(?:\w+\.)?checkio.org/(?:[^\/]+)/mission/(?P<id>[^\/]+)\/?",
)
BEECROWD_EXERCISE_URL = re.compile(
    r"(?:www\.)?beecrowd\.com\.br/judge/(?:[\w_-]+)/problems/(view\/)?(?P<id>\d+)\/?",
)
EXERCISE_URLS = [CHEKIO_EXERCISE_URL, BEECROWD_EXERCISE_URL]


def print_dict(data, bullet="*", **kwargs):
    """
    Pretty print mapping-like data as a list of items.
    """
    print = config.console.print
    for k, v in data.items():
        print(f"[b][green]{bullet}[/] {k}[green]:[/][/]", v, **kwargs)


def extract_kwargs(kwargs: dict[str, Any], keys: Iterable[str]) -> dict[str, Any]:
    """
    Extract keys from list and return the projection of kwargs in
    the given key set.

    IMPORTANT: the extracted keys are removed from the input dictionary.
    """
    out = {}
    for key in keys:
        try:
            out[key] = kwargs.pop(key)
        except KeyError:
            continue
    return out


def url_to_username_normalizer(
    regex: Pattern,
) -> Callable[[Optional[str]], Optional[str]]:
    def fn(user: str | None) -> str | None:
        if not user:
            return None
        if user.startswith("@"):
            return user[1:]
        if m := URL_START.match(user):
            user = user[m.end() :]
        if m := regex.fullmatch(user):
            return m.group("user")
        return user

    return fn


def url_normalizer(
    item: str, *regex_list: Pattern
) -> Callable[[Optional[str]], Optional[str]]:
    def fn(slug: str | None) -> str | None:
        if not slug:
            return None
        if m := URL_START.match(slug):
            slug = slug[m.end() :]

            for regex in regex_list:
                if m := regex.fullmatch(slug):
                    return m.group(item)
        return slug

    return fn


user_url_normalizer = url_normalizer("user", *USER_URLS)
exercise_url_normalizer = url_normalizer("id", *EXERCISE_URLS)


@singledispatch
def number(obj) -> Number:
    raise TypeError(f"invalid type: {type(obj).__name__}")


@number.register(float)
def _(x):
    try:
        if int(x) == x:
            return int(x)
    except ValueError:  # inf and NaN
        pass
    return x


number.register(int, lambda x: x)
number.register(str, lambda x: number(float(x)))
number.register(type(None), lambda _: 0)  # type: ignore
