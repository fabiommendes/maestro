import random


#
# Examples
#
class ExamplesMeta(type):
    def __getitem__(cls, item):
        meta = cls.__class__
        return meta(cls.__name__, (cls,), {'examples': item})

    def __instancecheck__(cls, instance):
        return instance in cls.examples


class RandomExamplesMixin:
    "Adds the random() method to class"

    examples = []

    @classmethod
    def random(cls):
        return random.choice(cls.examples)


class Examples(RandomExamplesMixin, metaclass=ExamplesMeta):
    """
    A value from a set of examples.

    Usage:

        >>> examples = [1, 2, 3]
        >>> Examples[examples].random() in [1, 2, 3]
        True
    """


#
# External data sources
#
class LineOfMeta(ExamplesMeta):
    def __getitem__(cls, path):
        with open(path) as F:
            lines = F.readlines()
        examples = [line.rstrip('\n') for line in lines]
        new = super().__getitem__(examples) 
        new.path = path
        return new


class LineOf(RandomExamplesMixin, metaclass=LineOfMeta):
    """
    Examples are stored in the given file path.

    Given a file with the contents::

        foo
        bar

    We obtain random values from

        >>> LineOf['examples.test'].random() in ['foo', 'bar']
    """
