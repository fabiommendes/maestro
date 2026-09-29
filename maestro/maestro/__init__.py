import builtins


def _stop(msg):
    raise ValueError(msg)


builtins.stop = _stop  # type: ignore
