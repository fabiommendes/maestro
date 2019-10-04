import pytest

from maestro.course.compilers import check_regex

simple_spec = """
POSITIVE:
ababab

NEGATIVE:
abba
"""


class TestRegexTester:
    def test_simple_example(self):
        check_regex(r'(ab)+', simple_spec)

    def test_validates_regex_size(self):
        check_regex(r'(ab)+', simple_spec, max_size=5)

        with pytest.raises(AssertionError):
            check_regex(r'(ab)+', simple_spec, max_size=3)


class TestRegexCrosswords:
    pass
