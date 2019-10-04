import sidekick as sk


class FuzzyMatcher:
    """
    Fuzzy match 2 lists of strings.
    """
    given: list
    real: list
    shape = property(lambda self: (len(self.given), len(self.real)))

    def __init__(self, given, real):
        self.given = list(given)
        self.real = list(real)
        self.distances = {}

    def remove_exact(self, aliases=None):
        """
        Remove all exact matches from matcher.
        """
        exact = {}
        for x in list(self.given):
            if x in self.real:
                self.remove_pair(x, x)
                exact[x] = [x]

        # Check if any alias is present
        if aliases:
            for x in list(self.given):
                if x in aliases:
                    ys = aliases[x]
                    for y in ys:
                        try:
                            self.remove_pair(x, y)
                        except ValueError:
                            pass
                    exact[x] = ys
        return exact

    def remove_pair(self, a, b):
        """
        Remove pair if present in matcher.
        """
        if b in self.real:
            self.given.remove(a)
            self.real.remove(b)
            self.distances.pop((a, b), None)
        else:
            raise ValueError

        self.distances = {
            (a_, b_): d
            for ((a_, b_), d) in self.distances.items()
            if a != a_ and b_ != b
        }

    def remove_given(self, a):
        self.given.remove(a)
        self.distances = {(a_, b): d
                          for ((a_, b), d) in self.distances.items()
                          if a != a_}

    def set_distance_threshold(self, value):
        """
        Remove all pairs whose distance is larger than or equal the given
        threshold.
        """
        for pair, dist in list(self.distances.items()):
            if dist >= value:
                del self.distances[pair]

    def fill_distances(self):
        """
        Fill distance matrix.
        """
        matrix = self.distances
        for x in self.given:
            for y in self.real:
                matrix[x, y] = pair_distance(x, y)
        return matrix

    def _fill_distances_non_empty(self):
        if not self.distances:
            self.fill_distances()

    def closest_pair(self):
        """
        Return the pair of values with the smallest distance.
        """

        self._fill_distances_non_empty()
        return min(self.distances, key=self.distances.get)

    def best_matches(self, given, n):
        """
        Provide the n best matches for the given value.
        """
        self._fill_distances_non_empty()
        return sorted(
            ((b, d) for ((a, b), d) in self.distances.items() if a == given),
            key=lambda x: x[1]
        )[:n]


def set_distance(a, b, sep='_'):
    sa = set(a.split(sep))
    sb = set(b.split(sep))
    return len(sa.symmetric_difference(sb)) / len(sa.union(sb))


def pair_distance(a, b, sep='_'):
    a = a.split(sep)
    b = b.split(sep)
    sa = set(bigrams(a)).union(a)
    sb = set(bigrams(b)).union(b)
    return len(sa.symmetric_difference(sb)) / len(sa.union(sb))


def bigrams(lst):
    return list(sk.window(2, lst))
