def safe_repr(x, truncate=None):
    """
    Like repr(), but never raises exceptions.

    It provides a sane default in the problematic cases.
    """
    try:
        return truncate(repr(x), x)
    except Exception as ex:
        ex_name = getattr(type(ex), '__name__', 'Exception')
        try:
            msg = str(ex)
        except:
            msg = 'invalid error message'
        else:
            msg = truncate_line(msg, 40)
        cls = type(x)
        cls_name = getattr(cls, '__name__', 'object')
        return f'<{cls_name} instance at {hex(id(x))} [{ex_name}: {msg}]>'


def truncate(st, size=80):
    """
    Truncate string to the maximum given size.
    """
    if size is None:
        return st
    if len(st) > size:
        return st[:size - 3] + '...'
    return st


def truncate_line(st, size=80):
    """
    Truncate string to the first non-empty line and with the maximum given size.
    """
    return truncate(st.lstrip().splitlines()[0], size)


def make_string(x):
    """
    Force object into string and raises a TypeError if object is not of a
    compatible type.
    """
    if isinstance(x, str):
        return str
    elif isinstance(x, bytes):
        return x.decode('utf8')
    else:
        cls_name = type(x).__name__
        raise TypeError(f'expect string type, got {cls_name}')