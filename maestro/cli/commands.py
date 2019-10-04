import json
import os
import zipfile
from pathlib import Path

import click

from maestro.cli.config import TomlConfigFile
from .fuzzy_match import FuzzyMatcher
from .utils import dict_merge


class NbCommand:
    """
    Abstract class for many notebook management commands.
    """

    @classmethod
    def exec(cls, *args, **kwargs):
        """
        Execute task while creates it.
        """
        task = cls(*args, **kwargs)
        task.run()
        return task

    def __init__(self, path, config_path):
        self.path = path
        self.config_path = config_path
        self.invalid_files = {}
        self.config = TomlConfigFile(Path(config_path) / 'config.toml')

        try:
            self.zipfile = zipfile.ZipFile(self.path)
        except zipfile.BadZipFile as ex:
            msg = click.style(str(ex), bold=True, fg='red')
            click.echo(msg, err=True, color=True)
            exit()

    #
    # Public methods
    #
    def run(self):
        """
        Abstract implementation.
        """
        raise NotImplementedError

    def load_files_info(self):
        """
        Return the authors list.
        """
        authors = {}
        paths = sorted(self.zipfile.filelist, key=lambda x: x.date_time)
        for path in paths:
            with self.zipfile.open(path) as fd:
                try:
                    data = json.load(fd)
                    names = self.extract_author_name(data)
                except json.JSONDecodeError:
                    fd.seek(0)
                    self.invalid_files[fd.name] = fd.read()
                else:
                    authors.update({name: path for name in names})

        return authors

    def update_config(self, data):
        """
        Update config file with the given data.
        """
        self.config.data = dict_merge(self.config.data, data)
        self.config.save()


class ExtractAuthorsFromZipFile(NbCommand):
    """
    Extract authors from zip file of submissions.
    """

    def run(self):
        """
        Load files asking interactively.
        """
        authors = sorted(set(self.load_files_info()))

        # Show list of authors
        click.echo('List of users:')
        for n, author in enumerate(authors, start=1):
            click.echo(f'{n}) {author}')

        # Clean list and return
        remove = self._ask_remove()

        return [x[1] for x in enumerate(authors) if x not in remove]

    def _ask_remove(self):
        remove = click.prompt('Remove (comma separated)',
                              default='', show_default=False)
        numbers = remove.strip().split(',')
        try:
            return [int(x) - 1 for x in numbers if x]
        except ValueError:
            click.echo('Invalid input!')
            return self._ask_remove()


class CheckAuthors(NbCommand):
    """
    Match authors to registered authors
    """

    def __init__(self, path, config_path, name=None, category=None):
        super().__init__(path, config_path)
        self.pending = {}
        self.files_map = {}
        self.name = name
        self.category = category

    def run(self):
        """
        Match all given names with the corresponding real values.
        """

        config = self.config

        # Start fuzzy matcher
        files = self.load_files_info()
        real = config.get('students.ids', [])
        matcher = FuzzyMatcher(files.keys(), real)

        # Remove certain matches
        author_map = matcher.remove_exact(config.get_all_student_aliases())
        matcher.fill_distances()
        matcher.set_distance_threshold(0.90)

        # Match each missing author with the given real name
        while matcher.shape[0] != 0 and False:
            given_, real_ = matcher.closest_pair()
            click.echo(f'\nBest match for {given_}')
            matches = self.ask_matches(given_, matcher.best_matches(given_, 5))

            if matches:
                for match in matches:
                    matcher.remove_pair(given_, match)
                    config.add_student_aliases(match, [given_])
                    author_map[given_] = match
            else:
                matcher.remove_given(given_)

        # Save files
        read_zip = lambda x: self.zipfile.open(x).read()

        for k, f in files.items():
            if k in author_map:
                data = read_zip(f.filename)
                for name in author_map[k]:
                    path = Path(f'submitted/{name}/{self.category}/{self.name}.ipynb')
                    path.parent.mkdir(parents=True, exist_ok=True)
                    if not os.path.exists(path):
                        with open(path, 'wb') as fd:
                            fd.write(data)

    def ask_matches(self, value, options):
        """
        Associate match for value from list of options.
        """
        # Fast track dominant matches
        if len(options) == 1 or options[1][1] - options[0][1] > 0.5:
            match = options[0][0]
            if click.confirm(f'Confirm as {match}?', default=True):
                return [match]

        # Print menu
        for i, (m, pc) in enumerate(options, start=1):
            pc = int(100 * (1 - pc))
            click.echo(f' {i}) {m} ({pc}%)')
        click.echo(f'\n {i + 1}) Add new')
        click.echo(f' {i + 2}) Ignore')

        # Process value
        while True:
            try:
                opts = self.ask_options('Choose option', len(options), -1)
            except ValueError:
                click.echo('Invalid option!')
                continue

            if opts == 'add-new':
                self.config.add_student(value)
                return [value]

            elif opts == 'ignore':
                return []
            else:
                return opts

    def ask_options(self, msg, n_options, delta=0):
        """
        Ask for one or more options in a list of n elements.
        """
        opt_max = n_options
        new_option = n_options + 1
        ignore_option = n_options + 2
        res = map(int, click.prompt(msg, type=str).split(','))

        if res == [new_option]:
            return 'add-new'
        elif res == [ignore_option]:
            return 'ignore'
        elif all(1 <= n <= opt_max for n in res):
            return [n + delta for n in res]


class LoadFile(NbCommand):
    def __init__(self, path, config_path, name=None):
        super().__init__(path, config_path)
        self.pending = {}

        # Save name
        if name is None:
            name = os.path.splitext(os.path.basename(path))[0]
        self.name = name

        # Authors map
        self.files_info = self.load_files_info()
        self.author_map = {}

    def run(self):
        self.check_authors(self.files_info.keys(), self.config.data['students']['ids'])

        # for author, name in authors.items():
        #     print('author', author, 'name', name)

        if self.invalid_files:
            self.handle_invalid_files()

    def handle_invalid_files(self):
        n = len(self.invalid_files)
        error_path = os.path.join(self.config_path, 'error')
        click.echo(f'{n} invalid files were saved at {error_path}/')

        try:
            os.mkdir(error_path)
        except FileExistsError:
            pass

        for name, data in self.invalid_files.items():
            name = os.path.basename(name)
            with open(os.path.join(self.config_path, 'error', name), 'wb') as fd:
                fd.write(data)
