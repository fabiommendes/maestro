import random

MAX_INT = 2 ** 65


class NumberMeta(type):
    def __getitem__(cls, idx):
        a, b = idx
        if a < b:
            raise ValueError('first index should be smaller than second!')
        ns = dict(minimum=a, maximum=b)
        return NumberMeta(cls.__name__, (cls,), ns)

    def __instancecheck__(cls, instance):
        return isinstance(instance, cls._type)


class NumberBase(metclass=NumberMeta):
    """Base class for Int and Float"""

    minimum = ...
    maximum = ...


class Int(NumberBase):
    """A random integral number"""

    _type = int

    @classmethod
    def random(cls):
        min, max = cls.minimum, cls.maximum
        min = -MAX_INT if min is ... else min
        max = MAX_INT if max is ... else max

        return random.randint(min, max)


class Float(NumberBase):
    _type = float
