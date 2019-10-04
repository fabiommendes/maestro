import operator as op
from types import MappingProxyType

from sidekick import Result, itertools

from ..utils.text import safe_repr, make_string


def check_sync(f1, f2, test_cases, check_all=False, kwargs=False, simeq=False):
    """
    Checks if function f1 and f2 produces the same output for all the given
    inputs

    Args:
        f1, f2:
            Functions that should be compared. In error messages, f1 is treated
            as the source of truth and f2 as the test function.
        test_cases (list):
            List of arguments to apply to f1 and f2. Tuple arguments are
            expanded as multiple arguments
        check_all (bool):
            If True, collect the list of all errors and raise an exception
            which displays all problems found.
        kwargs (bool):
            If True, assumes that args is a list of args tuples and kwargs
            dictionaries. Any of the two elements may be missing.
        simeq (bool, callable):
            If True, accepts similar, but not identical results. This is
            generally a good idea when testing floating point outputs. It can
            also be a callable that receives each result and test them for
            approximate equality.

    Examples:
        >>> from math import tan, sin, pi
        >>> check_sync(tan, sin, [0, pi, 2*pi], simeq=True)                   # Pass!
        >>> check_sync(tan, sin, [0, pi/2, pi, 3/2 * pi, 2*pi], simeq=True)   # Fails!

    Raises:
        Raises an AssertionError if any two outputs are different.
    """

    if simeq is False:
        simeq = op.eq

    errors = []

    for args, kwargs in map(normalize_args(kwargs), test_cases):
        res1 = Result.call(f1, *args, **kwargs)
        res2 = Result.call(f2, *args, **kwargs)
        error = Result.first_error(res2, res1)
        if error is None:
            cmp = Result.apply(simeq, res1, res2)
            if cmp.is_err:
                error = cmp.error
            elif cmp.value is False:
                expect = safe_repr(res1.value, 20)
                got = safe_repr(res2.value, 20)
                args = format_call_args(args, kwargs, safe_repr)
                error = f'for f({args}), expect: {expect}, got: {got}'

            if error is not None:
                if check_all:
                    errors.append(error)
                else:
                    raise AssertionError(format_error(error))

    if check_all and errors:
        msg = '\n'.join(map(line_prefix('    * '), map(format_error, errors)))
        raise AssertionError('Many errors were found:\n', msg)


def normalize_args(kwargs):
    def normalizer(x) -> (tuple, dict):
        if kwargs:
            if isinstance(x, tuple):
                if (len(x) == 2
                        and isinstance(x[0], tuple)
                        and isinstance(x[1], dict)):
                    return x
                return x, {}
        elif isinstance(x, tuple):
            return x, {}
        else:
            return (x,), {}

    return normalizer


def format_error(err) -> str:
    if isinstance(err, str):
        return err
    else:
        cls = type(err).__name__
        msg = str(err)
        return f'{cls}: {msg}'


def format_call_args(args=(), kwargs=MappingProxyType({}), repr=repr):
    """
    Format the args and kwargs of a function signature. using safe_repr
    """
    return ', '.join(itertools.chain(
        map(repr, args),
        (f'{k}={repr(v)}' for k, v in kwargs.items())
    ))


#
# String formatting (move to sidekick)
#
CURRIED = object()


def line_prefix(item, st: str = CURRIED):
    """
    Add prefix to the first line of string. Following lines are indented to
    the number of spaces equal to the prefix.
    """
    if st is CURRIED:
        return lambda x: line_prefix(item, x)

    size = len(item)
    return item + indent(size, st, skip_first=True)


def indent(size, st: str = CURRIED, skip_first=False):
    """
    Indent text with the given indent. Indent can be a string or a number that
    represents the number of spaces that should prefix each line.
    """
    if st is CURRIED:
        return lambda x: indent(size, x, skip_first)

    pre = ' ' * size if isinstance(size, int) else make_string(size)
    lines = iter(st.splitlines())
    prefix = (lambda x: pre + x)

    if skip_first:
        first = next(lines)
        lines = itertools.chain([first], map(prefix, lines))
    else:
        lines = map(prefix, lines)

    return '\n'.join(lines)


