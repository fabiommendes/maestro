import re
import random
import functools
import string

__all__ = ['random_string']

parser = re.sre_compile.sre_parse
parse = parser.parse


def random_string(pattern):
    """
    Return a random string from the given regex pattern.
    """
    return ''.join(randomizer(pattern))


class RandomChars:
    """
    Provides an iterator that creates a random sequence of characters from a 
    regex.

    This class is just an implementation detail. Users should stick to the 
    public random_string() function. 
    """

    def __init__(self, pattern):
        self.pattern = pattern if isinstance(pattern, str) else pattern.pattern
        self.parsed = parse(self.pattern)

    def __iter__(self):
        for item in self.parsed:
            yield from self.yield_from_item(item)

    def yield_from_item(self, item):
        head, args = item

        try:
            method = getattr(self, 'yield_' + head.name)
        except AttributeError:
            raise ValueError('not supported: %s' % head)

        try:
            yield from method(args)
        except:
            print('head: %s, args: %s' % (head, args))
            raise

    def yield_MAX_REPEAT(self, args):
        start, end, pattern = args
        if end is parser.MAXREPEAT:
            end = start + 256

        # Required initial repetitions
        for _ in range(start):
            item = random.choice(pattern)
            yield from self.yield_from_item(item)

        # Additional repetitions
        max_size = end - start
        size = min(int(random.expovariate(0.3)), max_size)
        size = min(size, 10)
        for _ in range(size):
            item = random.choice(pattern)
            yield from self.yield_from_item(item)

    def yield_IN(self, items):
        item = random.choice(items)
        yield from self.yield_from_item(item)

    def yield_CATEGORY(self, cat):
        method = getattr(self, 'random_' + cat.name[9:].lower())
        yield str(method())

    def yield_RANGE(self, range_):
        a, b = range_
        return chr(random.randrange(a, b))

    def yield_LITERAL(self, value):
        return chr(value)

    def yield_BRANCH(self, value):
        arg, patterns = value
        if arg is not None:
            raise NotImplementedError('BRANCH: %s' % value)
        sub_pattern, = random.choice(patterns)
        yield from self.yield_from_item(sub_pattern)

    def yield_SUBPATTERN(self, value):
        group, _, _, data = value
        for item in data:
            yield from self.yield_from_item(item)   
    
    # Randomizers
    def random_digit(self):
        return random.randint(0, 9)

    def random_letter(self):
        return random.choice(string.ascii_letters)

    def random_word(self):
        r = random.random()

        if r < 0.3:
            return self.random_digit()
        elif r < 0.7:
            return self.random_letter()
        else:
            return random.choice(['_'])
        return random.randint(0, 9)


@functools.lru_cache(256)
def randomizer(pattern):
    """
    Return an initialized RandomChars instance.
    """
    return RandomChars(pattern)
