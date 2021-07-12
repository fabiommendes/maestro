import codecs
import os
from logging import getLogger
from pathlib import Path
from typing import MutableMapping

import sidekick.api as sk

import fred

NOT_GIVEN = object()
DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true'
log = getLogger('maestro')


def conf(key, default=NOT_GIVEN, conf=None, env=None, type=str):
    """
    Retrieve configuration key from config.
    """

    if conf is None:
        conf = global_config

    try:
        for k in key.split('.'):
            conf = conf[k]
        return conf
    except KeyError:
        if default is not NOT_GIVEN:
            return default
        if env is not None and env in os.environ:
            return type(os.environ[env])
        raise KeyError(key)


def set_conf(key, value, conf=None):
    """
    Set configuration key.
    """
    if conf is None:
        conf = global_config

    *base, name = key.split('.')
    data = conf
    for k in base:
        data = data.setdefault(k, {})
    data[name] = value
    conf.save()


class Conf(MutableMapping):
    """
    Expose JSON configuration settings as a dictionary.
    """
    _open = open  # Those must be available when cleaning Conf instances
    _load = staticmethod(fred.load)
    _dump = staticmethod(fred.dump)
    _ascii = codecs.lookup('ascii')

    _data = property(lambda self: self._read())
    items = sk.delegate_to('_data')
    keys = sk.delegate_to('_data')
    values = sk.delegate_to('_data')

    def __init__(self, path=None, **kwargs):
        self._cache = None
        self._path = path or self._default_path()
        if kwargs:
            self.update(**kwargs)

    def __getitem__(self, key):
        return self._read()[key]

    def __setitem__(self, key, value):
        data = self._read()
        data[key] = value
        self._write(data)

    def __iter__(self):
        return iter(self._read())

    def __len__(self):
        return len(self._read())

    def __delitem__(self, key):
        data = self._read()
        del data[key]
        self._write(data)

    def __repr__(self):
        data = ', '.join(f'{k}={v!r}' for k, v in self.items())
        if data:
            data = ', ' + data
        return f'Conf({str(self._path)!r}{data})'

    def __del__(self):
        self.save()

    def _read(self):
        if self._cache is None:
            with self._open(self._path, 'r') as fd:
                self._cache = self._load(fd)
        return self._cache

    def _write(self, data):
        with self._open(self._path, 'w') as fd:
            self._dump(data, fd)

    def _default_path(self):
        path = Path('~/.config/maestro/conf.fred').expanduser()
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            log.info(f'Creating empty config: {path}')
            path.write_text('{}')
        return path

    def save(self):
        if self._cache is not None:
            self._write(self._cache)


global_config = Conf()

if __name__ == '__main__':
    from pprint import pprint

    print('Global configuration')
    pprint(dict(global_config))
