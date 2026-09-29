"""
Modify Python's runtime to run with restrictions.

For complete support, each language should implement a module similar to this.
"""
import builtins
from contextlib import contextmanager
from typing import Mapping
from functools import lru_cache

GLOBALS = globals()
BUILTINS = builtins.__dict__.copy()
_RuntimeError = RuntimeError


class Runtime:
    def __init__(self):
        self._env = vars(builtins)

    def filter_imports(self, *, forbid=None, allow=None):
        """
        Filter imports either by allowing from a whitelist or
        by forbiding some blacklist.
        """
        original = GLOBALS["__import__"]

        if forbid is not None and allow is not None:
            raise TypeError("cannot specify both forbid and allow keyword arguments")

        is_allowed = lambda _: True
        if forbid:
            is_allowed = lambda name: name not in forbid
        if allow:
            is_allowed = lambda name: name in allow

        def __import__(name, globals, locals, fromlist, level):
            if not is_allowed(name):
                raise ImportError(f"{name} is not allowed")
            return original(name, globals, locals, fromlist, level)

        self._env["__import__"] = __import__

    def filter_builtin_functions(self, *, forbid=None, allow=None):
        """
        Filter builtins either by allowing from a whitelist or
        by forbiding some blacklist.
        """

    def globals(self) -> dict:
        """
        Return the execution environment dictionary.
        """
        return self._env.copy()

    def global_patches(self):
        """
        Harden the execution environment globally.

        Unlike patches on context managers, those modifications
        cannot be undone.
        """
        global_patches()

    @contextmanager
    def local_patches(self):
        """
        A context manager that applyies local patches and reverts them on exit.
        """
        with patch_builtins(self._env), patch_sys():
            yield

    def eval(self, src, *, globals=None, locals=None):
        """
        Evaluate the source code with restrictions.
        """
        if globals is None:
            globals = self.globals()
        else:
            for k, v in self.globals():
                globals.setdefault(k, v)

        self.global_patches()
        with self.local_patches():
            return eval(src, globals, locals)

    def exec(self, src, *, globals=None, locals=None):
        """
        Execute the source code with restrictions.
        """
        if globals is None:
            globals = self.globals()
        else:
            for k, v in self.globals():
                globals.setdefault(k, v)

        self.global_patches()
        with self.local_patches():
            exec(src, globals, locals)


def locked_function(func):
    """
    Return a function with compatible signature and similar introspection
    properties as the original function, but raises a RuntimeError when
    called.
    """
    msg = f"{func.__name__} is locked"

    def locked(*args, **kwargs):
        raise _RuntimeError(msg)

    for attr in ["name", "doc", "module", "qualname", "annotations", "signature"]:
        if hasattr(func, attr):
            setattr(locked, attr, getattr(func, attr))

    return locked


# ============================================================================
# Reversible patches context managers.
# ============================================================================
@contextmanager
def patch_builtins(replacements: Mapping):
    """
    Patch builtins with the given functions.
    """
    try:
        yield
    finally:
        ...


@contextmanager
def patch_sys():
    """
    Patch the sys module with a more secure restricted version.
    """
    try:
        yield
    finally:
        ...


# ============================================================================
# Irreversible patches
# ============================================================================
@lru_cache(1)
def global_patches():
    """
    Harden the execution environment globally.
    """
