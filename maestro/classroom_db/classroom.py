import datetime as dt
import json
import os
from typing import List, Optional

import toml
from unidecode import unidecode

from maestro.classroom_db.queryable import Queryable
from sidekick import Record, lazy


class Student(Record):
    """
    A student with meta information.
    """
    name: str
    school_id: str = None
    email: str = None
    aliases: List[str] = lazy(lambda _: [])

    @property
    def display(self):
        display = self.name
        if self.school_id:
            display += f' ({self.school_id})'
        return display

    def __init__(self, name: str, school_id=None, email=None, aliases=()):
        assert isinstance(name, str), name
        super().__setattr__('name', name)
        super().__setattr__('school_id', school_id)
        super().__setattr__('email', email)
        super().__setattr__('aliases', list(aliases))

    def __repr__(self):
        data = repr(self.name)
        if self.school_id:
            data = f'{data}, {self.school_id}'
        if self.email:
            data = f'{data}, email={self.email!r}'
        if self.aliases:
            data = f'{data}, aliases={self.aliases!r}'
        return f'{self.__class__.__name__}({data})'

    # Derived properties
    @property
    def normalized_name(self):
        name = self.name.strip(' \t\n\r\f\'"-/:_()[]{}')
        return unidecode(name.lower().replace(' ', '_'))

    @classmethod
    def from_json(cls, data):
        return cls(**data)

    def to_json(self, nbgrader=False):
        if nbgrader:
            first_name, *rest = self.name.split()
            last_name = ' '.join(rest)
            return {
                'id': self.school_id,
                'first_name': first_name,
                'last_name': last_name,
                'email': self.email,
            }
        return {
            'name': self.name, 'school_id': self.school_id,
            'email': self.email, 'aliases': self.aliases,
        }


class Students(Queryable):
    """
    A collection of students.

    Can query, modify and persist results.
    """

    @classmethod
    def from_json(cls, data):
        if data is None:
            return cls()
        return cls(map(Student.from_json, data))

    def to_json(self):
        return [x.to_json() for x in self.data]

    def add(self, student):
        """
        Adds student in database.
        """
        if student.school_id is None:
            raise ValueError('cannot add element with null school_id')

        school_id = student.school_id
        if any(s for s in self.data if s.school_id == school_id):
            raise ValueError('Student with given id already exists on database')
        self.data.append(student)

    def by_alias(self, alias) -> Student:
        """
        Return student by alias.
        """
        return self.filter_by_alias(alias).single()

    def by_pk(self, pk):
        return self.filter(lambda x: x['school_id'] == pk).single()

    def filter_by_alias(self, alias, inplace=False) -> 'Students':
        """
        Return elements with the given alias.
        """
        gen = (s for s in self.data if alias in s.aliases)
        return self._from_generator(gen, inplace)

    def best_match(self, student) -> Optional[Student]:
        """
        Return the best match for the given student object or None if no good
        match was found.
        """
        best = []
        school_id = student.school_id or object()
        ref = student.normalized_name

        for std in self.data:
            if std.school_id == school_id:
                return std
            if std.normalized_name == ref:
                best.append((std, 10))

        best.sort(key=lambda x: x[1], reverse=True)
        if best:
            return best[0][0]
        return None


class Classroom:
    """
    Describes a classroom.
    """
    name: str = None
    created: dt.datetime
    teacher: str = None
    students: Students = None
    assignments: list = None
    path: str = None

    @classmethod
    def load(cls, path) -> 'Classroom':
        """
        Load classroom from toml or json file.
        """
        try:
            with open(path) as fd:
                mod = get_db_module(path)
                data = mod.load(fd)
        except FileNotFoundError:
            data = {}

        out = cls.from_json(data)
        out.path = path
        return out

    @classmethod
    def from_json(cls, data) -> 'Classroom':
        """
        Initialize classroom from JSON-like data created with the to_json()
        method.
        """
        info = data.get('classroom', {})
        out = cls(info.get('name', 'classroom'),
                  info.get('teacher', 'teacher'),
                  assignments=info.get('assignments', []))
        out.students = Students.from_json(data.get('students'))
        out.created = info.get('created', dt.datetime.now())
        return out

    def __init__(self, name, teacher, students=(), assignments=(), **kwargs):
        self.name = name
        self.teacher = teacher
        self.students = Students(students)
        self.created = dt.datetime.now()
        self.assignments = list(assignments)

        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
            else:
                raise TypeError(f'invalid argument: {k}')

    def to_json(self):
        return {
            'classroom': {
                'name': self.name,
                'teacher': self.teacher,
                'created': self.created,
                'assignments': self.assignments,
            },
            'students': self.students.to_json()
        }

    def save(self, path=None):
        path = path or self.path
        if not path:
            raise ValueError('path must be specified.')

        with open(path, 'w') as fd:
            mod = get_db_module(path)
            mod.dump(self.to_json(), fd)

        return self


def get_db_module(path):
    ext = os.path.splitext(path)[-1]
    if ext == '.json':
        return json
    elif ext == '.toml':
        return toml
    else:
        raise ValueError(f'invalid extension: {ext}')
