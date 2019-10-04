import builtins
from contextlib import contextmanager

from sidekick import record


@contextmanager
def update_builtins(**kwargs):
    """
    Context manager that temporarily sets the specified builtins to the given
    values.

    Examples:
        >>> with update_builtins(print=lambda *args: None) as orig:
        ...     print('Hello!')          # print is shadowed here
        ...     orig.print('Hello!')  # Calls real print
    """

    undefined = object()
    revert = {k: getattr(builtins, k, undefined) for k in kwargs}
    try:
        for k, v in kwargs.items():
            setattr(builtins, k, v)
        yield record(**{k: v for k, v in revert.items() if v is not undefined})
    finally:
        for k, v in revert.items():
            if v is not undefined:
                setattr(builtins, k, v)
