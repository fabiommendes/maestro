import os
from subprocess import run
from typing import List, Optional

import pandas as pd
from sidekick import Record

import ezio as io

REPO_TRY_NAMES = ['Repo', 'repo', 'Git', 'git', 'Url', 'url']
BRANCH_TRY_NAMES = ['Branch', 'branch']
PATH_TRY_NAMES = ['Path', 'path', 'Name', 'name', 'Id', 'ID', 'id']


class Repo(Record):
    """
    Basic information about a repository.
    """

    url: str
    path: Optional[str] = None
    depth: Optional[int] = None
    branch: Optional[str] = 'master'

    @classmethod
    def from_record(cls, data):
        """
        Create record from object that has the same attributes as Repo.
        """
        return cls(url=data.url,
                   path=getattr(data, 'path', None),
                   depth=getattr(data, 'depth', None),
                   branch=getattr(data, 'branch', 'master'))

    def clone(self, echo=False, log=print):
        """
        Clone repository.
        """
        cmd = self.clone_command()
        if echo:
            log(' '.join(cmd))
        return run(cmd)

    def clone_command(self) -> List[str]:
        """
        Return the git clone command.
        """
        cmd = ['git', 'clone', self.url]
        if self.path:
            cmd.append(self.path)
        if self.depth:
            cmd.extend(['--depth', str(self.depth)])
        if self.branch:
            cmd.extend(['--branch', self.branch])
        return cmd


def clone_repos_at(source, repo=None, path=None, branch=None, depth=None,
                   default_branch='master'):
    """
    Clone all repositories in the given source path.
    """

    info = pd.DataFrame()
    _, ext = os.path.splitext(source)
    if ext == '.csv':
        df = pd.read_csv(source)
    else:
        raise RuntimeError(f'Invalid file type: {ext!r}')

    # Repository location
    if repo is None:
        for col in REPO_TRY_NAMES:
            if col in df.columns:
                info['url'] = df[col]
                break
        else:
            raise RuntimeError('No repository given or found in data source.')
    else:
        info['url'] = df[repo]

    # Branch data
    if branch is None:
        for col in BRANCH_TRY_NAMES:
            if col in df.columns:
                info['branch'] = df[col]
                break
        else:
            info['branch'] = None
    else:
        info['branch'] = df['branch']
    info['branch'] = info.branch.fillna(default_branch)

    # Depth
    info['depth'] = None if depth is None else int(depth)

    # Output name
    if path is None:
        for col in PATH_TRY_NAMES:
            if col in df.columns:
                info['path'] = df[col]
                break
        else:
            info['path'] = info.url.apply(repo_name)
    else:
        info['path'] = df[path]

    repos = list(info.apply(Repo.from_record, axis=1))
    return clone_repos(repos)


def clone_repos(repos: List[Repo], echo=True, update=False):
    """
    Clone all repositories in list.
    """
    for repo in repos:
        if os.path.exists(repo.path) and not update:
            io.print(f'<red>Skipping repo:</red> <b>{repo.path}</b>', format=True)
        else:
            repo.clone(echo=echo)


def repo_name(url):
    """Normalize repository name from git url."""

    data = url.rstrip('/').split('/')[-2:]
    return '__'.join(data)
