import argparse


def repr_fcall(fname, args, kwargs):
    """Nice string representation for function call"""

    data = ', '.join(map(repr, args))
    data += ', '.join('%s=%r' % item for item in kwargs.items())
    return '%s(%s)' % (fname, data)


def function_grader(mod, func_name, examples, print=print):
    """
    Grades a function using a list of examples.
    """
    try:
        func = getattr(mod, func_name)
    except AttributeError:
        msg = 'Error: module does not have a %r function [grade = 0%%]'
        raise SystemExit(msg % func_name)

    n_ok = 0
    n_items = len(examples)
    print('Grading %s examples...' % n_items)

    for args, kwargs, expected in examples:
        correct = False

        try:
            result = func(*args, **kwargs)
            correct = result == expected
        except Exception as ex:
            error_name = ex.__class__.__name__
            fcall = repr_fcall(func_name, args, kwargs)
            print('  %s at %s: %s' % (error_name, fcall, ex))
        else:
            if correct:
                n_ok += 1
            else:
                fcall = repr_fcall(func_name, args, kwargs)
                msg = '  wrong: %s -> %r, expected %r.'
                print(msg % (fcall, result, expected))

        # Final summary
        if n_ok == n_items:
            print('Congratulations! All %s tests passed!' % n_ok)
        else:
            pc = 100 * n_ok / n_items
            print('Success rate: %.0f%% (%d/%d)' % (pc, n_ok, n_items))
