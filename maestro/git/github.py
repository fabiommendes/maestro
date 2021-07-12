from logging import getLogger
from pathlib import Path

import requests
import sidekick.api as sk
from github import Github, InputFileContent
from github.File import File

from ..conf import conf

log = getLogger('maestro')


def IS_NOT_PRIVATE(file: Path):
    return not file.name.startswith('.')


def download_pull_requests(repo_name, path='.', auth=None, state='all',
                           include_files=IS_NOT_PRIVATE, force_download=False,
                           include_repos=lambda x: True):
    """
    Download all pull requests into path.

    Args:
        repo_name:
            Repository name;
        path:
            Path in which PR files will be saved.
        auth:
            Authentication token as passed to the :func:`github` function.
        include_files:
            A predicate function that selects which paths will be included
            in the  download. Can pass a collection of objects instead of a
            callable.
        include_repos:
            A predicate function that selects which repo keys will be included
            in the download. Can pass a collection of objects instead of a
            callable.
        force_download:
            If true, re-downloads downloaded files.
    """
    if not callable(include_files):
        include_files = sk.to_callable(include_files)

    gh = github(auth)
    repo = gh.get_repo(repo_name)
    path = Path(path)

    for pr in repo.get_pulls(state=state):
        key = f'{pr.user.login}-{pr.id}'
        if not include_repos(key):
            continue

        dirname = path / key

        gh_file: File
        n_files = 0
        for gh_file in pr.get_files():
            file = Path(gh_file.filename)

            if not include_files(file):
                continue

            file_path = dirname / file
            file_path.parent.mkdir(parents=True, exist_ok=True)
            if force_download or not file_path.exists():
                response = requests.get(gh_file.raw_url)
                log.info(f'download: [{gh_file.raw_url}]')
                file_path.write_bytes(response.content)
                n_files += 1

        log.info(f'PR:[{repo_name}:{pr.user.login}] downloaded {n_files} files')

        yield {
            "path": str(dirname),
            "key": key,
            "id": pr.id,
            "created": pr.created_at,
            "modified": pr.last_modified,
            "user": {
                "username": pr.user.login,
                "name": pr.user.name,
                "email": pr.user.email,
            },
            "steps": {"download": {"id": pr.id, "title": pr.title, "url": pr.html_url}},
        }


def simple_gist(file, content, auth=None, public=True):
    """
    Create a simple gist with a single file.
    """
    gh = github(auth)
    user = gh.get_user()
    return user.create_gist(public, {file: InputFileContent(content)})


def github(auth=None) -> Github:
    """
    Normalize authentication and return an initialized Github instance.
    """
    if auth is None:
        token = conf('github.token', None)
        if token:
            return Github(token)
        username = conf('github.username')
        password = conf('github.password', env='MAESTRO_GITHUB_PASSWORD')
        return github((username, password))

    if isinstance(auth, (tuple, list)):
        return Github(*auth)
    elif isinstance(auth, str):
        return Github(auth)

    raise NotImplementedError


if __name__ == '__main__':
    download_pull_requests('compiladores-fga/calc-parser', '.')
