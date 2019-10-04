def dict_merge(a, b):
    a_ = dict(a)
    _dict_merge(a_, b)
    return a_


def _dict_merge(a, b):
    # Merges b into a *inplace*
    for kb, vb in b.items():
        if isinstance(vb, dict):
            try:
                va = a[kb]
            except KeyError:
                a[kb] = vb.copy()
            else:
                _dict_merge(va, vb)
        else:
            a[kb] = vb