import re
import pytest
from maestro.regex import random_string


@pytest.fixture(params=[
    # Simple
    r'a', r'\n', r'\[', r'\]', r'\$', r'\^',

    # Categories
    r'\w', r'\d',

    # Examples
    r'[0-9]', r'a|b',

    # Sequences
    r'ab', r'[a-z][0-9]',

    # Repetitions
    r'[0-9]?', r'[0-9]{1}', r'[0-9]{1,10}',

    # Groups
    r'(ab)|(cd)', r'(ab)', r'(?P<foo>ab)',
])
def regex(request):
    pattern = request.param
    return re.compile(pattern)


def test_random_string_accept_regex_or_string():
    pattern = r'a'
    regex = re.compile(pattern)
    assert random_string(pattern) == random_string(regex)


def test_random_strings_fully_match_pattern(regex):
    tested = set()

    # Let us not waste too much time on tests that yield always the same result
    for _ in range(10):
        st = random_string(regex)
        assert regex.fullmatch(st)
        tested.add(st)

    if len(tested) > 2:
        for _ in range(100):
            st = random_string(regex)
            assert regex.fullmatch(st)
