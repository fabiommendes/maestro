import base64
from functools import singledispatch
from hashlib import blake2b
from types import FunctionType
from typing import Union, Any, Optional

import dill
import sys
import zlib
from sidekick import pipe

Secret = Union[bytes, str]
EMPTY = object()
PREFIX = {
    (True, True): b'@',  # is pickled, is string
    (False, True): b'%',  # hash, is string
    (True, False): b'$',  # is pickled, bytes
    (False, False): b'#',  # hash, bytes
}


def secret(obj, serialize=False, readable=False, type=None) -> Secret:
    """
    Create secret encoding of object.

    Args:
        obj:
            Object to be hidden
        serialize (bool):
            If True, uses pickle to serialize object in a way that it can be
            reconstructed afterwards. Usually this is not necessary and we
            simply compute a hash of the object.
        readable:
            If readable, return a b85-encoded string. Otherwise, return bytes.
        type:
            Coerce to type before computing secret.

    Returns:
        A bytestring representing the secret representation.

    See Also:
        :func:`check_secret` - verify if object is compatible with secret.
    """
    prefix = b':'
    if type:
        prefix = type_prefix(type)
        obj = coerce_type(obj, type)
    pickled = dill.dumps(obj)

    if serialize:
        data = zlib.compress(pickled)
    else:
        data = blake2b(pickled, digest_size=16).digest()

    prefix = PREFIX[serialize, readable] + prefix
    if readable:
        b85 = prefix + base64.b85encode(data)
        return b85.decode('ascii')
    return prefix + data


def check_secret(obj: Any, code: Secret) -> bool:
    """
    Executes function serialized as string. Function receives a dictionary
    with the global namespace and must raise AssertionErrors to signal that a
    test fails.
    """

    code = _as_bytes(code)
    typ = type_from_prefix(code.partition(b':')[0][1:])
    if not code:
        raise ValueError('empty secret')
    if code[0] in b'#%':
        code_ = secret(obj, type=typ)
        return code_ == code
    else:
        other = decode_secret(code, add_globals=False)
        return coerce_type(obj, typ) == other


def decode_secret(secret: Secret, *, add_globals=True) -> Any:
    """
    Extract object from secret.

    Args:
        secret:
            String containing encoded secret.
        add_globals (bool):
            For functions encoded using a secret, it adds the globals
            dictionary to function's own __code__.co_globals.
    """

    secret = _as_bytes(secret)
    if not secret:
        raise ValueError('empty secret')
    if secret[0] in b'#%':
        raise ValueError('cannot decode a hash-based secret')
    elif secret[0] not in b'@$':
        raise ValueError(f'not a valid secret: {secret}')

    obj = zlib.decompress(dill.loads(secret[1:]))
    if add_globals is not False and isinstance(obj, FunctionType):
        # noinspection PyUnresolvedReferences
        frame = sys._getframe(2 if add_globals is True else add_globals)
        obj.__code__.co_globals = frame.f_globals

    return obj


def _as_bytes(st: Secret) -> bytes:
    if isinstance(st, bytes) and st.startswith(b'#'):
        return st
    elif isinstance(st, bytes) and st.startswith(b'%'):
        prefix, _, data = st.partition(b':')
        return b'#%b:%b' % (prefix[1:], base64.b85decode(data))
    return pipe(st.encode('ascii'), _as_bytes)


def type_prefix(typ) -> bytes:
    """
    Return the prefix string for the given type.

    The prefix determine the type of the expected object.
    """
    return b'o'


def coerce_type(obj: Any, typ: Optional[type]) -> Any:
    """
    Coerce object to the given type.

    Type must be registered for this function to work.
    """
    if typ is None or typ is type(obj):
        return obj

    fn = _coerce_fn.dispatch(typ)
    result = fn(obj)

    if result is EMPTY:
        t1 = type(obj).__name__
        t2 = typ.__name__
        raise TypeError(f'object of type {t1} cannot be coerced to {t2}')

    return result


def type_from_prefix(prefix: bytes) -> Optional[type]:
    """
    Return coercion type from bytestring prefix.

    Return None for empty prefixes
    """
    if not prefix:
        return None
    try:
        return COERCE_TYPES[prefix]
    except KeyError:
        raise ValueError(f'invalid prefix: {prefix}')


@singledispatch
def _coerce_fn(_):
    """
    Implements single dispatch mechanism for type. The function is not meant to
    be called directly as it is used as a dispatch dictionary rather than a regular
    function.
    """
    return EMPTY


BASIC_TYPES = [int, float, str, complex, list, tuple, dict, set, frozenset]
for _typ in BASIC_TYPES:
    _coerce_fn.register(_typ)((lambda fn: lambda _, x: fn(x))(_typ))

TYPE_PREFIXES = {typ: typ.__name__[0].encode('ascii') for typ in BASIC_TYPES[:-2]}
TYPE_PREFIXES.update({set: b'S', frozenset: b'F'})
COERCE_TYPES = {v: k for k, v in TYPE_PREFIXES.items()}


def create_main(command=None, run=False):
    """
    Return the main function as a standalone command or sub-command.
    """
    import click

    command = command or click
    secret_fn = secret

    @command.command(name='secret')
    @click.argument('cmd')
    @click.option('--type', '-t', help='Type prefix (i)nt, (f)loat, (c)omplex, etc.')
    @click.option('--pickle', '-p', is_flag=True, help='Serialize as a pickled element')
    @click.option('--secret', '-s', help='Compare value with this secret')
    @click.option('--bytes', '-b', is_flag=True, help='Print as bytes')
    def main(cmd, type=None, pickle=False, secret=None, bytes=True):
        try:
            code = compile(cmd, '<input>', 'eval')
        except SyntaxError:
            raise SystemExit('Invalid python expression')
        else:
            value = eval(code)

        if type is not None:
            type = type_from_prefix(type.encode('ascii'))

        code = secret_fn(value, readable=not bytes, type=type, serialize=pickle)
        if secret:
            if not (secret and secret[0] in '$#%@'):
                mode = ('rb' if secret[0] in '#$' else 'r')
                with open(secret, mode) as fd:
                    secret = fd.read().rstrip()

            if check_secret(value, secret):
                click.echo('OK')
                return exit(0)
            else:
                return exit(f'Invalid secret, got: {code}')

        if isinstance(code, str):
            click.echo(code)
        else:
            sys.stdout.buffer.write(code)
        return exit(0)

    if run:
        return main()
    return main


if __name__ == '__main__':
    create_main(run=True)
