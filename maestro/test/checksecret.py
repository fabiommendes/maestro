import base64
import sys
import zlib
from hashlib import blake2b
from types import FunctionType

import dill

from sidekick import pipe


def secret(obj, serialize=False):
    """
    Create secret encoding of object.

    Args:
        obj:
            Object to be hidden
        serialize (bool):
            If True, uses pickle to serialize object in a way that it can be
            reconstructed afterwards. Usually this is not necessary and we
            simply compute a hash of the object.

    Returns:
        A string representing the secret representation.
    """
    pickled = dill.dumps(obj)

    if serialize:
        data = zlib.compress(pickled)
        prefix = '$'
    else:
        data = blake2b(pickled, digest_size=16)
        prefix = '#'
    data = base64.b85decode(data).decode('ascii')
    return f'{prefix}{data}'


def check_secret(obj, secret):
    """
    Executes function serialized as string. Function receives a dictionary
    with the global namespace and must raise AssertionErrors to signal that a
    test fails.
    """

    if secret.startswith('#'):
        assert secret(obj) == secret, 'Objects do not match'
    else:
        ref = decode_secret(secret, add_globals=False)
        assert obj == ref, 'Objects do not match'


def decode_secret(secret, *, add_globals=True):
    """
    Extract object from secret.

    Args:
        secret:
            String containing encoded secret.
        add_globals (bool):
            For functions encoded using a secret, it adds the globals
            dictionary to function's own __code__.co_globals.
    """

    if not secret.startswith('$'):
        raise ValueError('not a valid secret')

    obj = pipe(
        secret[1:].encode('ascii'),
        base64.b85decode,
        zlib.decompress,
        dill.loads,
    )

    if add_globals is not False and isinstance(obj, FunctionType):
        frame = sys._getframe(2 if add_globals is True else add_globals)
        obj.__code__.co_globals = frame.f_globals

    return obj
