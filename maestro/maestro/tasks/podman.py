from __future__ import annotations

import hashlib
import json
import shlex
import subprocess
from base64 import b64encode
from collections import deque
from pathlib import Path
from typing import MutableMapping

from ..repo import AnnotatedPath, Repo, b64digest
from ..utils import rich_render
from .subprocess import Execution, create_subprocess, run_subprocess


async def build(
    cwd: Path,
    options: MutableMapping[str, str],
    tag: str = "maestro-image",
    container_file: Path = Path("Containerfile"),
) -> str:
    """
    Build a Podman container image.

    Args:
        tag:
            The container image tag.
        container_file:
            The path to the Containerfile (default is "Containerfile" in the cwd).

    Returns:
        A string with the podman image.
    """
    lines = deque(["Unexpected error!", "podman build did not run."], 2)
    image = options.get("podman-image")
    cached_hash = options.get("podman-build")
    computed_hash = build_hash(cwd, container_file)

    if image and cached_hash == computed_hash:
        return image

    cwd = cwd.absolute()
    cmd = [
        "podman",
        "build",
        ".",
        *["-t", tag],
        *["-f", str(container_file.resolve().relative_to(cwd))],
    ]
    process = create_subprocess(cmd, cwd)
    result = await run_subprocess(process, on_line=lines.append)

    if isinstance(options, AnnotatedPath):
        data = result.cmd_string + "\n\n" + result.read()
        options.log("podman-build", data)

    # Usually, the two last lines are:
    #   Successfully built|tagged <tag>
    #   <image_id>
    msg, image = lines
    if not msg.lower().startswith("successfully "):
        raise RuntimeError("\n".join([msg, image]))

    options.update(
        {
            "podman-image": image,
            "podman-build": computed_hash,
        }
    )
    return image


async def run_submission(
    cwd: Path,
    image: str,
    cmd: str = "",
) -> Execution:
    """
    Run a Podman container image in a submission repository to test its
    contents.

    Args:
        cwd:
            The path to the repository to mount in the container.
        image:
            The name of the image to run.
        cmd:
            The command to run inside the container (default is an empty string, which runs the default
            command defined in the container image).
        notify:
            Whether to notify the user about the run process.
        name:
            The name of the run task, used for logging.
    """

    exec = [] if cmd == "" else shlex.split(cmd)
    cwd = cwd.resolve().absolute()
    podman_cmd = [
        "podman",
        "run",
        "--rm",
        "--tty",
        *["-v", ".:/submission/"],
        image,
        *exec,
    ]
    process = create_subprocess(podman_cmd, cwd=cwd)
    result = await run_subprocess(
        process,
        valid_codes=None,
        check=False,  # We generally expect the test command to fail
    )
    return result


async def autograde(
    repo: Repo,
    image: str,
    expect: Path | str,
    cmd: str = "",
    exit_codes: list[int] = [0, 1],
    force: bool = False,
) -> bool:
    """
    Run a an autograding task in a Podman container.

    The container should produce an output file that is expected to
    exist in the repository after grading. This file is specified by the
    `expect` parameter.

    The grading file is later used to determine the grading results in the
    pipeline.

    Args:
        repo:
            The repository to run the autograding in.
        image:
            The name of the Podman image to run.
        cmd:
            The command to run in the container. If empty, use the default
            command from the image.
        expect:
            The path to a file that is expected to exist in the repository
            after grading.
        expected_codes:
            A list of valid exit codes from the grading command.
        force:
            If True, force the autograding even if the grading file already
            exists.

    Returns:
        The path to the expected grading file, if it exists or None.
    """
    if isinstance(expect, Path) and expect.is_absolute():
        expect_file = expect
    else:
        expect_file = repo.path / expect

    # We assign a hash to know when it is safe to skip correction.
    hasher = hashlib.md5()
    hasher.update(repo.digest())
    hasher.update(image.encode("utf-8"))
    grading_hash = hasher.hexdigest()
    stored_hash: str | None = repo.meta.get("podman-autograde", None)

    # Cache results to avoid regrading if the grading file is already present
    # and nothing has changed in the repository hash.
    if not force and stored_hash is not None:
        prefix, _, status = stored_hash.partition(":")
        if prefix == grading_hash:
            return status in ("ok", "")

    result = await run_submission(
        cwd=repo.path,
        image=image,
        cmd=cmd,
    )

    # If the grading file exists, we consider the grading successful.
    # Even failed attempts may be accepted, if they fail with an expected exit
    # code. This can happen, for instance, if the testing command detected
    # some problem such as a missing file or syntax error.
    if not expect_file.exists():
        repo.meta["podman-autograde"] = grading_hash + ":failed"
        return False

    # We raise an exception for unexpected errors
    elif result.code not in exit_codes:
        id = rich_render(f"[bold red]{repo.id}[/]")
        msg = (
            f"Autograding {id} failed.\n"
            f"Expected grading file '{expect}' does not exist after autograding.\n"
            f"The grading command failed with exit code {result.code}:\n\n"
            f"  $ {result.cmd_string}\n"
        )
        raise ValueError(msg)

    repo.meta["podman-autograde"] = grading_hash + ":ok"
    return True


def image_from_tag(tag: str) -> str:
    """
    Extract the image name from a Podman image tag.

    Args:
        tag: The tag of the image, e.g., "maestro-image:latest".

    Returns:
        The image name, e.g., "maestro-image".
    """

    result = subprocess.run(
        ["podman", "images", tag, "--format", "json"],
        capture_output=True,
        text=True,
    )
    tags = {tag, f"{tag}:latest", f"localhost/{tag}:latest"}
    for info in json.loads(result.stdout):
        if set(info["History"]).intersection(tags):
            return info["Id"]

    raise ValueError(f"Image {tag} not found in podman images.")


def build_hash(path: Path, container_file: Path, cache: bool = True) -> str:
    """
    Compute a hash for the Podman build based on the contents of the
    repository and the Containerfile.
    """

    digests = [
        b64digest(path, cache=cache).encode("ascii"),
        b64digest(container_file, cache=cache).encode("ascii"),
    ]
    hasher = hashlib.md5()
    for digest in digests:
        hasher.update(digest)
    return b64encode(hasher.digest()).decode("ascii")
