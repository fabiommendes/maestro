import threading


def safe_exec(code, globs=None, locals=None, timeout=None):
    """
    Similar to the builtin exec() function, but has some security measures.

    Differently from the builtin, it returns the resulting timeout.

    Args:
        code:
            Code, or source object.
        globs, locals:
            Optional dictionaries of globals and locals.
        timeout (float):
            Optional timeout in seconds.
    """
    if globs is None:
        globs = {}
    if locals is None:
        locals = globs
    if timeout is not None:
        with_timeout(timeout, exec, code, globs, locals)
    else:
        exec(code, globs, locals)
    return locals


def with_timeout(*args, **kwargs):
    """
    Exec function with the given timeout

    Args:
        timeout:
            Timeout in seconds.
        func:
            Function to be executed.

    Examples:
        >>> with_timeout(5, sum, [1, 2, 3])
        6
    """
    timeout, func, *args = args
    result_container = []

    def wrapped():
        result_container.append(func(*args, **kwargs))

    thread = threading.Thread(target=wrapped)
    thread.start()
    thread.join(timeout)
    if result_container:
        return result_container[0]
    raise ValueError('function could not produce output')
