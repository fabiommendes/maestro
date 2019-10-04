import copy
import dis
from inspect import Signature
from types import FunctionType

import bytecode

from sidekick import placeholder as _, pipe, map
from ..utils.exec import safe_exec

LOAD_FAST = dis.opmap['LOAD_FAST']
LOAD_NAME = dis.opmap['LOAD_NAME']
STORE_FAST = dis.opmap['STORE_FAST']
STORE_NAME = dis.opmap['STORE_NAME']
DELETE_FAST = dis.opmap['DELETE_FAST']
DELETE_NAME = dis.opmap['DELETE_NAME']
transformer = {
    LOAD_FAST: LOAD_NAME,
    STORE_FAST: STORE_NAME,
    DELETE_FAST: DELETE_NAME,
}


def check_locals(*args, **_variables):
    """
    Checks if function executed with the given positional arguments defines
    all local variables passed as keyword arguments.

    This function does not return anything, but it raises AssertionErrors if
    some variables have an non-expected value.

    This function works by introspecting the input function bytecode and
    comparing the expected values to the value of the local dictionary after
    execution is completed.

    Args:
        func:
            Function to be executed or a tuple with the function function
            followed by its positional arguments.

    Examples:
        >>> def my_func(x):
        ...     y = x + 1
        ...     return y
        >>> check_locals((my_func, 40), y=42)
    """

    func, *kwargs = args
    kwargs, = kwargs if kwargs else {},
    if not callable(func):
        func: FunctionType
        func, *args = func
        arg_names = pipe(
            Signature.from_callable(func)
                .parameters.keys(),
            map(_.name))
        kwargs.update(zip(arg_names, args))

    code = bytecode.Bytecode(func.__code__)
    code = fast_to_name(code)
    code = code.to_code()
    ns = safe_exec(code, locals=kwargs)
    for name, value in _variables.items():
        assert name in ns, f'Variable {name} not found'
        assert ns[name] == value, f'Expect: {value!r}, got: {ns[name]!r}'


#
# Auxiliary functions
#
def fast_to_name(lst):
    """
    Replaces all *_FAST opcodes into *_NAME codes and disable locals
    optimization on the resulting bytecode.
    """
    result = copy.copy(lst)
    result.clear()

    for instr in lst:
        code = instr.opcode
        instr.opcode = transformer.get(code, code)
        result.append(instr)

    result.flags = result.flags & (~bytecode.CompilerFlags.OPTIMIZED)
    return result
