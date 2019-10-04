import re


class RegexTesterMeta(type):
    """
    Metaclass for regex-tester classes.
    """


Re = type.__new__(RegexTesterMeta, (), {'regex': r''})


def make_examples(re_class, accept=5, reject=5):
    """
    Takes a regex class and make a list of accept/reject examples.
    """

# The teacher
class Integer:
    """
    Match any valid positive integer.
    """
    regex = r'[0-9]+'
    ok = ['42', '0', '1']
    bad = ['foo', '41.0']


# For the students
class Integer:
    """
    Match any valid positive integer.
    
    Good:
        42
        0  
        1
    
    Bad:
        foo  
        41.0
    """
    regex = r''


def test_class(cls, data):
    """
    Test a regex class definition. 
    
    Return None if class was not tested and a tuple (n_ok, n_error) with the
    number of correct/wrong test cases.
    """
    cls_examples = data[cls.__name__]
    title = cls.__name__.replace('_', ' ')
    
    # Compile regex
    try:
        regex = re.compile(cls.regex)
    except AttributeError:
        print('%s: class does not define a regex attribute.')
        return None
    except Exception:
        print('%s: invalid regular expression.')
        return None

    # Test each suite of examples
    accept = cls_examples['accept']
    reject = cls_examples['reject']
    n_ok = 0
    msgs = []
    
    for case in accept:
        if not regex.fullmatch(case):
            msgs.append('did not match %r.' % case)
    for case in reject:
        if regex.fullmatch(case):
            msgs.append('match %r, but should have rejected it.' % case)

    # Render message
    if msgs:
        print('%s:' % title)
        print('  correct: %s' % n_ok)
        print('  wrong:')
        for msg in msgs:
            print('    -', msg)
    else:
        print('%s: ok!' % title)

    return n_ok, len(msgs)
