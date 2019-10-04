import json
import os
import re
import zipfile
from hashlib import md5
from pathlib import Path

from typing import Optional

import ezio
import sidekick as sk
from ezio import fs
from ezio import print as echo, input as ask
from maestro.classroom_db import Students
from ..classroom_db import Classroom, Student


class File(sk.Union):
    """
    Represents a loaded file.
    """
    Ok: sk.Case(name=str, data=dict)
    Err: sk.Case(name=str, data=bytes, msg=str)


def load_zip(zip_path: Path, config_path: Path, name: str, category: str):
    """
    Load files from given zip file at config path and save in the corresponding
    locations in the submitted folder.
    """

    echo = ezio.print
    ask = ezio.input
    classroom = Classroom.load(Path(config_path) / 'config.toml')
    students: Students = classroom.students
    extract_names = AuthorExtractor().extract
    bad_files = []
    existing_files = []

    for file in load_zip_files(zip_path):
        msg = f'Loading data from file <blue><b>{file.name}</b></blue>'
        echo(msg, format=True, end='')

        if not file.is_ok:
            echo('.')
            bad_files.append(file.name)
            handle_bad_file(file, config_path)
            continue

        found_students = extract_names(file.data)
        echo(f' ({len(found_students)} students found).')

        for student in found_students:
            found = students.best_match(student)

            # Happy case
            if found:
                student = found

            # Student was not found in database: ask for inclusion
            elif student.school_id:
                msg = f'  <b>{student.display}</b> not found.\n  Add to database?'
                if ask(msg, type=bool, default=True, format=True):
                    students.add(student)
                    classroom.save()
                    echo()
                else:
                    echo('User skipped.')
                    continue

            # Student was not found and has no school id.
            # We need to ask a valid school id to the user.
            else:
                student = ask_for_valid_school_id(student.name, classroom)
                if not student:
                    continue

            # Save student data
            assert student.school_id, student
            save_path = os.path.join(config_path,
                                     'submitted',
                                     student.school_id,
                                     category,
                                     name + '.ipynb')
            if not os.path.exists(save_path):
                fs.write(save_path, json.dumps(file.data), make_parent=True)
            else:
                existing_files.append(save_path)

    # Global messages
    if existing_files:
        msg = '\n<yellow><b>WARNING!</b></yellow> Skipping existing files:'
        echo(msg, format=True)
        for file in existing_files:
            echo(f' * <b>{file}</b>', format=True)

    if bad_files:
        msg = '\n<red><b>ERROR!</b></red> The following files are invalid'
        echo(msg, format=True)
        for file in bad_files:
            echo(f' * <b>{file}</b>', format=True)


def load_zip_files(zip_path):
    """
    Iterate over all File's in the given zip file.
    """
    zd = zipfile.ZipFile(zip_path)
    paths = sorted(zd.filelist, key=lambda x: x.date_time)
    for path in paths:
        with zd.open(path, 'r') as fd:
            try:
                data = json.load(fd)
                yield File.Ok(fd.name, data)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                fd.seek(0)
                yield File.Err(fd.name, fd.read(), str(exc))


def handle_bad_file(file: File, config_path: Path):
    """
    Saves invalid files in the error folder and prints a message to user.
    """
    msg = '<red><b>ERROR!</b></red> Invalid file type/content.'
    echo(msg, format=True)

    # Save file
    fname, ext = os.path.splitext(os.path.basename(file.name))
    md5_hash = md5(file.data).hexdigest()
    path = config_path / 'error' / f'{fname.lower()}-{md5_hash}{ext}'
    ezio.fs.write(path, file.data, '-p')
    echo(f'File saved under {path}')


def ask_for_valid_school_id(name: str, classroom: Classroom) -> Optional[Student]:
    """
    Construct new valid student for classroom asking user for a valid school id.
    """
    msg = f'<b>{name}</b> not found.'
    echo(msg, format=True)

    while True:
        school_id = ask('School id: ', default=None)
        if school_id is None:
            if ask('Skip student? ', type=bool):
                echo()
                return None
            else:
                continue

        try:
            student = Student(name, school_id)
            classroom.students.add(student)
            classroom.save()
            return student
        except ValueError:
            echo(f'School id exists for {student.display}')
            if ask('Reuse? ', type=bool):
                student = classroom.students.get(school_id=school_id)
                student.aliases.append(student.name)
                classroom.save()
                echo()
                return None


class AuthorExtractor:
    """
    Extract author name from ipynb file.
    """
    id_re = re.compile(r'\d+/?\d+')

    def __call__(self, *args, **kwargs):
        return self.extract(*args, **kwargs)

    def extract(self, data):
        """
        Return a list of authors from ipynb data.
        """

        # Read authors from the cell with NAME and COLLABORATORS variables
        authors = []
        for cell in data['cells']:
            if cell['cell_type'] != 'code':
                continue

            src = cell['source']
            if src and src[0].startswith('NAME'):
                authors = [*self._extract_name(src, 'NAME'),
                           *self._extract_name(src, 'COLLABORATORS')]
                break

        return [x for x in authors if x.name]

    def _extract_name(self, src, variable='COLLABORATORS'):
        for line in src:
            if line.startswith(variable):
                _, _, name = line.partition('=')
                return [self._with_student_id(name.strip(' "\''))]
        return []

    def _with_student_id(self, name):
        m = self.id_re.search(name)
        if m:
            i, j = m.span()
            id_ = name[i:j].replace('/', '')
            name = name[:i] + name[j:]
        else:
            id_ = None

        for symb in '[]{}(),.;:-"\'\t\n\r\f/\\?@$%!':
            name = name.replace(symb, '')
        name = ' '.join(name.split())
        if name.isupper() or name.islower():
            name = name.title()
        student = Student(name, school_id=id_)
        student.aliases.append(student.normalized_name)
        return student
