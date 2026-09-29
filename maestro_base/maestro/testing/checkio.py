import io
import re
import sys
import typing as typ
from collections import deque
from collections.abc import Mapping
from functools import wraps

from ..utils.bultins import update_builtins

_print = print
_input = input
Spec = typ.Union[deque, dict, str]


def check_interaction(*args, **kwargs):
    """
    Checks if the given function called with *args and **kwargs produces the
    expected io.

    It does nothing in case of success or raises an AssertionError if the main
    function does not produces the expected io.

    Args:
        func (callable):
            Test function
        spec:
            A list of output/input strings. Inputs are represented by sets or
            lists with a single string entry. Each string entry corresponds to
            a line of output.

        Additional positional and keyword arguments are passed to func during
        execution.

    Examples:
        >>> def hello_name():
        ...     name = input('Name: ')
        ...     print('Hello %s!' % name)
        >>> spec = [
        ...     'Name: ', {'John'},
        ...     'Hello John!'
        ... ]
        >>> check_interaction(hello_name, spec)
    """
    func, spec, *args = args

    if isinstance(spec, str):
        spec = deque([spec])
    elif isinstance(spec, Mapping):
        spec, mapping = deque(), spec.items()
        for k, v in mapping:
            spec.extend([k, ignore_ws, v])
    else:
        spec = deque(spec)

    @wraps(_input)
    def input_(msg=None):
        if msg is not None:
            print_(msg, end='')
        if not spec:
            raise AssertionError('Unexpected input')

        res = spec.popleft(0)
        if _is_input(res):
            raise AssertionError('Expects output, but got an input command')

        # Extract input from singleton list or set
        try:
            value, = res
            return value
        except TypeError:
            raise ValueError('Expected input list must have a single value')

    @wraps(_print)
    def print_(*args_, file=None, **kwargs_):
        if file in (None, sys.stdout, sys.stderr):
            fd = io.StringIO()
            # noinspection PyTypeChecker
            _print(*args_, file=fd, **kwargs_)
            for line in fd.getvalue().splitlines():
                _consume_output(line, spec)
        else:
            _print(*args_, file=file, **kwargs_)

    with update_builtins(input=input_, print=print_):
        func(*args, **kwargs)

    assert not spec, f'Outputs/inputs were not exhausted: {spec}'


def make_spec(func, inputs, *args, **kwargs) -> deque:
    """
    Return the the io spec list that represents an interaction with the given
    function. Must pass a list of inputs or the Ellipsis (...) object for
    reading inputs interactively.

    Examples:
        >>> hello_name = ['Paul'].__iter__
        >>> make_spec(hello_name, ['Paul'])
        ['Name: ', {'Paul'}, 'Hello Paul!']

    See Also:
        :func:`check_io`: for comparing expected io with the result produced
        by a function.
    """

    spec = deque()
    if inputs is not ...:
        inputs = list(reversed(inputs))

    @wraps(_input)
    def input_(msg=None):
        if msg is not None:
            print_(msg, end='')
        if inputs is ...:
            value = _input()
        else:
            value = inputs.pop()
        spec.append([value])
        return value

    @wraps(_print)
    def print_(*args_, file=None, **kwargs_):
        if file in (None, sys.stdout, sys.stderr):
            fd = io.StringIO()
            # noinspection PyTypeChecker
            _print(*args_, file=fd, **kwargs_)
            spec.extend(fd.getvalue().splitlines())
            if inputs is ...:
                _print(*args_, **kwargs_)
        else:
            _print(*args_, file=file, **kwargs_)

    with update_builtins(input=input_, print=print_):
        func(*args, **kwargs)

    return spec


#
# Spec creation functions
#
def ignore_ws(received, spec):
    """
    Consume all whitespace in the beginning of the spec.

    No-op if first element does not start with whitespace.
    """
    if spec and isinstance(spec[0], str):
        spec[0] = spec[0].popleft().lstrip()
        _consume_output(received, spec)


def match(regex, test='full'):
    """
    Test if next string matches the given regular expression.
    """
    _re = re.compile(regex)
    attrs = {'full': 'fullmatch', 'start': 'match'}
    matches = getattr(_re, attrs[test])

    def regex_consumer(received, spec: deque):
        m = matches(received)
        if not m:
            raise AssertionError('Output does not match pattern')
        if test == 'start':
            remaining = received[m.end():]
            if remaining:
                spec.appendleft(remaining)

    return regex_consumer


#
# Auxiliary functions
#
def _is_input(obj):
    return not isinstance(obj, str) and not callable(obj)


def _consume_output(printed, spec: deque):
    """
    Helper function: consume the given output from io spec.

    Raises AssertionErrors when it encounter problems.
    """

    if not printed:
        return
    elif not spec:
        raise AssertionError('Asking to consume output, but expects no interaction')
    elif _is_input(spec[0]):
        raise AssertionError('Expects input, but trying to print a value')
    elif printed == spec[0]:
        spec.popleft()
    elif callable(spec[0]):
        spec.popleft()(printed, spec)
    elif spec[0].startswith(printed):
        spec[0] = spec[0][len(printed):]
    elif printed.startswith(spec[0]):
        n = len(spec.popleft())
        _consume_output(printed[n:], spec)
    else:
        raise AssertionError(f'Printed wrong value:\n'
                             f'    print: {printed!r}\n'
                             f'    got:   {spec[0]!r}')
