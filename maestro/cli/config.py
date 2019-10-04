from collections.abc import MutableMapping

import toml

from sidekick import lazy


class TomlConfigFile(MutableMapping):
    """
    Represents a TOML config file.
    """

    @lazy
    def data(self):
        try:
            return toml.load(open(self.path))
        except FileNotFoundError:
            return {}

    def __init__(self, path):
        self.path = path

    def __setitem__(self, key, value):
        self.data[key] = value

    def __delitem__(self, key):
        del self.data[key]

    def __getitem__(self, key):
        return self.data[key]

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def save(self):
        """
        Forces saving to disk.
        """
        toml.dump(self.data, open(self.path, 'w'))

    def get(self, path, default=None):
        """
        Like dict's get, but understand the dot notation to designate
        sub-elements.
        """
        if '.' in path:
            data = self.data
            *paths, last = path.split('.')
            for fragment in paths:
                data = data.get(fragment, {})
            return data.get(last, default)
        return super().get(path, default)

    def add_student(self, id_, save=True):
        """
        Register student id.
        """
        ids = self.data.setdefault('students', {}).get('ids', [])
        if id_ not in ids:
            ids.append(id_)
            ids.sort()

        if save:
            self.save()

    def get_student(self, id_):
        """
        Return information about student with given id.
        """
        return {'id': id_}

    def add_student_aliases(self, id_, aliases, save=True):
        """
        Register list of aliases for the given student id.
        """
        alias_db = self.data \
            .setdefault('students', {}) \
            .setdefault('aliases', {})

        for alias in aliases:
            lst = alias_db.setdefault(alias, [])
            if id_ not in lst:
                lst.append(id_)

        if save:
            self.save()

    def get_student_aliases(self, id_):
        """
        Return list of aliases for student.
        """
        return self.get(f'students.aliases.{id}', [])

    def get_all_student_aliases(self):
        """
        Return a mapping from alias to real ids for all students.
        """
        aliases = dict(self.get('students.aliases', ()))
        aliases.update({id_: [id_] for id_ in self.get('students.ids')})
        return aliases
