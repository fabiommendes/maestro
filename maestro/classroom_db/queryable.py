from collections.abc import Sized, Iterable
from typing import List

from sidekick import op


class Queryable(Sized, Iterable):
    """
    A queryable set of elements.
    """

    data: List['Queryable']

    #
    # Errors
    #
    class EmptyCollection(ValueError):
        """
        Raised on unexpected empty queries.
        """

    class MultipleValuesFound(ValueError):
        """
        Raised when more then the expected number of entries is found.
        """

    def __init__(self, data=()):
        self.data = list(data)

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def __getitem__(self, index):
        return self.by_pk(index)

    #
    # Abstract methods
    #
    def by_pk(self, pk):
        """
        Abstract method that implements how to extract elements by pk.
        """
        raise NotImplementedError

    #
    # API
    #
    def filter(self, key=None, *, inplace=False, **kwargs):
        """
        Filter collection by function.
        """
        gen = iter(self.data)
        if key is not None:
            gen = filter(key, gen)

        # Custom filters
        filters = []
        post_filters = []
        for k, v in kwargs.items():
            attr = f'filter_by_{k}'
            if hasattr(self, attr):
                post_filters.append((attr, v))
            elif callable(v):
                filters.append((k, v))
            else:
                filters.append((k, op.eq(v)))

        # Apply filters and compute result
        if filters:
            gen = (s for s in gen if all(f(getattr(s, k)) for k, f in filters))

        # Apply post filters
        qs = self._from_generator(gen, inplace)
        for (attr, v) in post_filters:
            qs = getattr(qs, attr)(v, inplace=inplace)

        return qs

    def map(self, func, *, inplace=False):
        """
        Map function to elements of collection.
        """
        return self._from_generator((func(s) for s in self.data), inplace)

    def first(self):
        """
        Return first element or raise ValueError if empty.
        """
        try:
            return self.data[0]
        except:
            raise ValueError

    def last(self):
        """
        Return last element or raise ValueError if empty.
        """
        try:
            return self.data[-1]
        except:
            raise ValueError

    def single(self):
        """
        Return element if collection has only one element, otherwise raise
        ValueError.
        """
        if len(self) == 1:
            return self.data[0]
        elif len(self) == 0:
            raise self.EmptyCollection('empty collection')
        else:
            raise self.MultipleValuesFound('multiple values found')

    def get(self, *args, inplace=None, **kwargs):
        """
        Like filter, but return a single element that satisfy query.

        Raises a ValueError if multiple elements where found.
        """
        if inplace is not None:
            raise TypeError('cannot set the inplace argument for get() queries')
        return self.filter(*args, **kwargs).single()

    #
    # Auxiliary methods
    #
    def _from_generator(self, gen, inplace):
        if inplace:
            self.data[:] = gen
            return self
        else:
            return type(self)(gen)
