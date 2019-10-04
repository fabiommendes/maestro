import io
import re
from typing import Pattern, Tuple, Union

from sidekick import lazy

TupleSt = Tuple[str, ...]


def check_regex(regex, spec, accept_empty=False, max_size=float('inf'), silent=False):
    """
    Check if regular expression accepts all "ok" strings and and refuses "negative"
    strings.

    Raises an AssertionError if regular expression is invalid.

    If negative is not given and ok is a string, it interpret as a multi-line string

    Args:
        regex (str):
            A string representing a regular expression.
        spec (str):
            A multiline string split into 2 sections with POSITIVE and NEGATIVE
            examples. Each example is in its own line.
        accept_empty (bool):
            If True, accepts empty strings.
        max_size (int):
            Expected size of string.
        silent (bool):
            If true, omit messages on success.
    """
    kwargs = {'accept_empty': accept_empty, 'max_size': max_size}
    return RegexTester.from_spec(regex, spec, **kwargs).run(silent=silent)


class RegexTester:
    """
    Test a group of strings against a regular expression.
    """

    _congratulations_message = "Congratulations! Your regex is correct :)"
    wrongly_accepted: TupleSt = lazy(lambda _: tuple(filter(_.rejects, _.positive)))
    wrongly_rejected: TupleSt = lazy(lambda _: tuple(filter(_.accepts, _.negative)))
    size: int = lazy(lambda _: len(_.pattern))
    regex: Pattern
    pattern: str
    positive: TupleSt
    negative: TupleSt
    size_multiplier: float
    max_size: Union[float, int]
    accept_empty: bool

    @classmethod
    def from_spec(cls, regex: str, spec: str, **kwargs):
        """
        Construct regex tester from spec.
        """
        kwargs.update(parse_spec(spec))
        return cls(regex, **kwargs)

    @lazy
    def grade(self) -> float:
        grade = 1.0 - bool(self.wrongly_accepted or self.wrongly_rejected)
        if not self.accept_empty and self.accepts(''):
            return 0.0
        if self.size > self.max_size:
            return self.size_multiplier * grade
        return grade

    def __init__(self, regex, positive=(), negative=(), max_size=float('inf'),
                 size_multiplier=0.75, accept_empty=False):

        if isinstance(regex, str):
            self.pattern = regex
            self.regex = re.compile(self.pattern)
        else:
            self.regex = regex
            self.pattern = self.regex.pattern

        self.positive = tuple(positive)
        self.negative = tuple(negative)
        self.size_multiplier = size_multiplier
        self.max_size = max_size
        self.accept_empty = accept_empty

    def accepts(self, st):
        """Check regular expression accepts string."""
        return self.regex.fullmatch(st) is not None

    def rejects(self, st):
        """Check if regular expression rejects string."""
        return not self.accepts(st)

    def run(self, silent=False, exception=AssertionError):
        """Check if regular expression passes all tests."""

        if self.grade == 1.0:
            if not silent:
                n = len(self.pattern)
                print(f'Congratulations! Valid regular expression with {n} characters.')
            return
        raise exception(self.message())

    def check_size(self, multiplier=None, silent=False, exception=AssertionError):
        """
        Checks only if size is correct.
        """
        if len(self.pattern) > self.max_size:
            raise exception(self.size_message(multiplier))
        elif not silent:
            print(self._congratulations_message)

    def message(self, skip_size=False):
        """
        Return an error or congratulatory message.
        """
        if self.grade == 1.0:
            return self._congratulations_message

        fd = io.StringIO()
        fd.write(f'Invalid regular expression: /{self.pattern}/\n\n')

        if not self.accept_empty and self.accepts(''):
            fd.write('Regular expression *must not* accept empty strings\n\n')

        self._warn_examples(fd, must='accept', at=self.wrongly_accepted)
        self._warn_examples(fd, must='reject', at=self.wrongly_rejected)

        skip_size = skip_size or self.max_size > len(self.pattern)
        if not skip_size:
            fd.write(self.size_message(congratulate=False))

        grade = int(100 * self.grade)
        fd.write(f"Your final grade is {grade}%.\n")

        return fd.getvalue()

    def size_message(self, congratulate=True):
        """
        Check if size is under budget and return an error or congratulatory
        message.
        """
        fd = io.StringIO()
        n = len(self.pattern) - self.max_size
        if n > 0:
            fd.write(f'Regex must be {n} character smaller to receive full grade.\n')
            fd.write('\n')
        elif congratulate:
            fd.write(self._congratulations_message)

        return fd.getvalue()

    def _warn_examples(self, fd, must, at):
        if at:
            fd.write(f'Regular expression must *{must}* the examples\n')
            for st in at:
                fd.write(f'  - {st!r}\n')
            fd.write('\n')
        else:
            fd.write(f'Regular expresion correctly *{must}s* all examples :)\n\n')


def parse_spec(spec: str) -> dict:
    """
    Return a map of {"positive": [<examples>], "negative": [<examples>]}

    Examples:
        >>> parse_spec('''
        ... POSITIVE:
        ... ab
        ... abab
        ...
        ... NEGATIVE:
        ... ba
        ... baba
        ... ''')
        {'positive': ['ab', 'abab'], 'negative': ['ba', 'baba']}                
    """
    _, sep1, spec = spec.strip().partition('POSITIVE:\n')
    positive, sep2, negative = spec.strip().partition('NEGATIVE:\n')
    if not sep1 or not sep2:
        raise ValueError('Spec does not contain a POSITIVE: and NEGATIVE: sections.')
    return {
        'positive': positive.strip().splitlines(),
        'negative': negative.strip().splitlines(),
    }
