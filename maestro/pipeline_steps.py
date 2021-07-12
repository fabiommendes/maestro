import json
import re
import subprocess
import time
from abc import ABC
from collections import defaultdict
from logging import getLogger
from pathlib import Path
from typing import Iterable, Tuple

import requests
from sidekick import api as sk

import fred

log = getLogger('maestro')

PYTEST_GRADE_AS_TEST_RATIO = lambda r: r['summary']['passed'] / r['summary']['total']


def from_timestamp(data):
    import pandas as pd
    return pd.to_datetime(data)


class Step(ABC):
    """
    Base class for all pipeline steps.
    """

    source = False
    collect = False

    def process(self, item: dict) -> dict:
        """
        Receives a dictionary representing a step data and return the
        corresponding result.
        """
        raise NotImplementedError('must be implemented in subclasses.')

    def pre_steps(self) -> Iterable[Tuple[str, 'Step']]:
        """
        Return a list of steps to be registered prior to current step.

        Pipeline doesn't recur this list, hence it is the responsibility of
        implementer to recur if necessary.

        Pre-steps of pipeline sources are ignored.
        """
        return ()

    def post_steps(self) -> Iterable[Tuple[str, 'Step']]:
        """
        Return a list of steps to be registered after current step.

        Pipeline doesn't recur this list, hence it is the responsibility of
        implementer to recur if necessary.
        """
        return ()


class Source(Step, ABC):
    """
    Base class for processing steps.
    """

    source = True

    def __init__(self, dest):
        self.dest = Path(dest)

    def collect(self) -> Iterable[Tuple[str, dict]]:
        """
        Collect all starting nodes of pipeline.
        """
        raise NotImplementedError

    def _open_ref(self, key, mode='r'):
        self.dest.mkdir(exist_ok=True, parents=True)
        return (self.dest / f'{key}.fred').open(mode)

    def ref(self, key):
        """
        Return data on key.
        """
        try:
            with self._open_ref(key) as fd:
                return fred.load(fd)
        except fred.FREDDecodeError:
            raise ValueError(f'invalid fred document for {key}')

    def update_steps(self, key, data):
        """
        Save step data produced for element.
        """
        conf = self.ref(key)
        conf['steps'].update(data)
        data = fred.dumps(conf, indent=2)

        with self._open_ref(key, 'w') as fd:
            fd.write(data)

        return conf

    def process(self, step):
        # Source elements usually do not have to process their generated nodes.
        pass


class PRs(Source):
    """
    Retrieve files from pull requests.
    """
    source = True

    def __init__(self, repo: str, dest: str = '.maestro_pipeline', auth=None,
                 include_paths=None, exclude_paths=None, refresh_every=2 * 60 * 60,
                 skip=False):
        super().__init__(dest)
        self.repo = repo
        self.skip = skip
        include_repos = sk.partial(self._reload, time.time() - refresh_every)
        self._data = {'auth': auth, 'include_repos': include_repos}
        self._skip = set()

        if include_paths:
            self._data['include_files'] = include_paths
        elif exclude_paths:
            self._data['include_files'] = ~sk.fn(sk.to_callable(exclude_paths))

    def _reload(self, deadline, key):
        conf_file = self.dest / f'{key}.fred'
        if not conf_file.exists():
            return True
        if not conf_file.stat().st_mtime < deadline:
            self._skip.add(key)
            return False
        return True

    def collect(self) -> Iterable[Tuple[str, dict]]:
        from .git.github import download_pull_requests

        for item in download_pull_requests(self.repo, self.dest, **self._data):
            item_conf_path = self.dest / f'{item["key"]}.fred'
            if not item_conf_path.exists():
                with item_conf_path.open('w') as fd:
                    fred.dump(item, fd, indent=2)
            else:
                with item_conf_path.open() as fd:
                    item = fred.load(fd)
            yield item['key'], item

        for key in self._skip:
            yield key, self.ref(key)


class CSV(Source):
    """
    Retrieve files from spreadsheet.
    """
    URL = re.compile(r'^https?://[^\n]+')
    GITHUB_URL = re.compile(r'^https?://github.com/(?P<repo>[^\n]+)')
    source = True
    DEFAULT_COLUMNS = {'Timestamp': 'created'}
    DEFAULT_TRANSFORMATIONS = {'created': from_timestamp}

    def __init__(self, source: str, columns=None, files=None,
                 dest: str = '.maestro_pipeline', transformations=None,
                 id='user.id', sort='created'):
        super().__init__(dest)
        self.files = files or {'file': 'data.txt'}
        self.id = id

        # Load dataframe
        import pandas as pd
        col_names = {**self.DEFAULT_COLUMNS, **columns}
        self.data = pd.read_csv(source).rename(columns=col_names)

        # Transformations
        fns = {**self.DEFAULT_TRANSFORMATIONS, **(transformations or {})}
        for col in self.data.columns:
            if col in fns:
                self.data[col] = self.data[col].apply(sk.to_callable(fns[col]))

        if sort:
            self.data.sort_values(sort, inplace=True)

    def collect(self) -> Iterable[Tuple[str, dict]]:
        repeated = set()

        for _, row in self.data.iloc[::-1].iterrows():
            key = str(row[self.id])
            if key in repeated:
                log.warning('Repeated submission: {key}')
                continue

            # Prepare data for persistence
            path = self.dest / key
            data = defaultdict(lambda: defaultdict(dict), steps={}, key=key, path=str(path))
            for k, v in row.items():
                if '.' in k:
                    group, k = k.split('.')
                    data[group][k] = v
                else:
                    data[k] = v

            # Save files to folder
            files = {}
            for col, fname in self.files.items():
                files[fname] = data.pop(col, '')
            self.save_files(key, files)

            try:
                with self._open_ref(key, 'r') as fd:
                    data = fred.load(fd)
            except FileNotFoundError:
                with self._open_ref(key, 'w') as fd:
                    fred.dump(data, fd, indent=2)

            yield key, data

    def save_files(self, key, data):
        directory = self.dest / key
        directory.mkdir(parents=True, exist_ok=True)

        for name, content in data.items():
            dest = directory / name
            if dest.exists():
                continue
            elif m := self.GITHUB_URL.match(content):
                repo = m.group('repo')
                url = f'https://raw.githubusercontent.com/{repo}/master/{name}'
                log.info(f'downloading submission from {url}')
                content = requests.get(url).text
            elif self.URL.match(content):
                url = content.strip()
                log.info(f'downloading submission from {url}')
                content = requests.get(url).text
            dest.write_text(content)


class IncludeFiles(Step):
    """
    Include files from a reference repository.
    """

    def __init__(self, path, files, overwrite=True):
        self.path = Path(path).expanduser()
        self.files = list(files)
        self.overwrite = overwrite

    def process(self, item: dict):
        log.info(f'Including files: [{item["key"]}]')
        files = []

        for file in self.files:
            src = (self.path / file).read_bytes()
            dest_path = Path(item["path"]) / file

            if dest_path.exists():
                if self.overwrite and dest_path.read_bytes() != src:
                    log.warning(f'overwriting user file: {item["key"]}:{file}')
                else:
                    continue
            dest_path.write_bytes(src)
            files.append(str(dest_path))

        return {'files': files, 'type': "IncludeFiles"}


class Pytest(Step):
    """
    Uses pytest to grade submissions.
    """

    def process(self, item: dict):
        path = Path(item['path'])
        log.info(f'Testing repository: [{item["key"]}]')

        cmd = 'pytest --json-report > .test.log'
        subprocess.run(cmd, shell=True, cwd=path)

        # Summarize
        with (path / ".report.json").open('r') as fd:
            result = self._clean_result(json.load(fd))

        summary = result.setdefault('summary', {})
        passed = summary.setdefault('passed', 0)
        failed = summary.setdefault('failed', 0)
        summary.setdefault('total', failed + passed)
        result['type'] = "Pytest"
        return result

    def _clean_result(self, result):
        return result


class Grader(Step):
    """
    Collect grades from previous steps.
    """

    def __init__(self, step, grade=PYTEST_GRADE_AS_TEST_RATIO):
        self.grade = grade
        self.step = step

    def process(self, item: dict) -> dict:
        result = item['steps'][self.step]
        return self.grade(result)


class Competencies(Step):
    """
    Collect competencies from previous steps.
    """

    def __init__(self, step, field='grade'):
        self.step = step
        self.field = field

    def process(self, item: dict) -> dict:
        competencies = item['steps'][self.step][self.field]
        return {c: True for c in competencies}


class SchoolId(Step):
    """
    Collect school id from submissions.
    """

    def __init__(self, filename=None, field=None, ref='user.id', db=None, transform=None):
        self.db = db or {}
        self.filename = filename
        self.field = field
        self.ref = ref
        self.transform = sk.to_callable(transform or (lambda x: x))

    def _parse_filename(self, path):
        if path.name.endswith('.py'):
            ns = {}
            exec(path.read_text(), ns)
        elif path.name.endswith('.json'):
            ns = json.loads(path.read_text())
        elif path.name.endswith('.fred'):
            ns = fred.loads(path.read_text())
        else:
            raise ValueError(f'invalid file type: {path.name}')

        return ns[self.field]

    def process(self, item: dict) -> dict:
        ref = data = self.get_ref(item)
        if self.filename:
            path = Path(item['path']) / self.filename
            if path.exists():
                data = self._parse_filename(path)

        return self.transform(self.db.get(ref, data))

    def get_ref(self, item):
        parts = self.ref.split('.')
        for p in parts:
            item = item[p]
        return item
