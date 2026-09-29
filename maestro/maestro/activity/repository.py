from __future__ import annotations

import copy
import datetime
import json
import shutil
from collections import defaultdict
from dataclasses import dataclass, field
from functools import cached_property, partial
from logging import getLogger
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Callable,
    Iterable,
    Iterator,
    Literal,
    cast,
)

import pandas as pd
import rich
import rich.progress
import rich.prompt
import trio
from pydantic import Field
from returns.result import Success

from maestro import yaml

from .. import tasks
from .. import types as T
from ..errors import ConfigurationError
from ..repo import AnnotatedPath, PathMetadata, Repo, RepoError
from ..tasks import gh
from ..tasks.plagiarism import (
    DuplicateFile,
    DuplicateResponse,
    Duplicates,
    find_duplicate_files,
    find_duplicate_responses,
)
from ..utils import (
    existing_folder,
    is_entry_point_name,
    load_entry_point,
    merge_dicts,
    merge_dicts_of_dicts,
    transpose_dicts,
)
from .base import BaseActivity
from .config import (
    Aggregator,
    FileConfig,
    PlagiarismOptions,
    PlagiarismPolicyItem,
    QuestionConfig,
    Transform,
)

NOT_GIVEN = NotImplemented

if TYPE_CHECKING:
    from ..classroom import Classroom
    from ..cli.sort_grading import SortGrading

    Classroom = Classroom

log = getLogger(__name__)


class Repository(BaseActivity):
    """
    Each activity is a repository with several files.

    Properties:
        submissions:
            An URI-like string specifing the source of submissions, e.g., a
            GitHub Classroom identifier, a zip file, etc. The accepted formats
            are:
            - `gh-classroom:<classroom_id>`: For GitHub Classroom of some
              specific id.
            - TODO: other formats!
        template:
            The path to the template directory, relative to the activity path.
            If ommited, it assumes the template is already initialized
            in the `template` directory.
        sandbox:
            The sandbox method to use for running the grading tasks. Currently,
            only "podman" is supported.
        files:
            A set of file patterns to be used for filtering files in the
            submissions. This is a dictionary with the following keys:
            - `exclude`: Patterns to exclude from the submissions.
            - `protect`: Patterns to protect from deletion.
            - `require`: Patterns that must be present in the submissions. It
               do not support glob patterns.
            - `expect`: Patterns that are expected to be present in the
              submissions, but not required.
    """

    type: Literal["repository"] = "repository"
    submissions: str
    template: str = ""
    sandbox: Literal["podman", "none"] = "podman"
    files: FileConfig = Field(default_factory=FileConfig)
    questions: QuestionConfig = Field(default_factory=QuestionConfig)
    autograde: list[str] | None = None
    answer_key: T.RepoId = T.repo_id("@answer-key")
    interactive: bool = True
    plagiarism: PlagiarismOptions = Field(default_factory=PlagiarismOptions)
    transforms: list[Transform] = Field(default_factory=list)
    # FIXME: gambira de ultima hora
    total: Literal["sum", "all-but-one"] = "sum"
    # Hidden fields
    manual_grading_options: dict[T.QuestionId, dict[str, Any]] = Field(
        default_factory=dict,
        exclude=True,
    )

    @property
    def template_folder(self) -> Path:
        """
        The path to the template directory.
        """
        return self.path / "template"

    @property
    def template_repo(self) -> Repo:
        """
        The template repository.
        """
        return Repo(self.template_folder)

    @property
    def submissions_folder(self) -> Path:
        """
        The path to the submissions directory.
        """
        return existing_folder(self.path / "submissions")

    @property
    def submission_errors_folder(self) -> Path:
        """
        The path to the submission errors directory.
        """
        return existing_folder(self.path / "submissions.errors")

    @property
    def cache_folder(self) -> Path:
        """
        The path to the cache folder.
        """
        return existing_folder(self.path / "cache")

    @property
    def scripts_folder(self) -> Path:
        """
        The path to the scripts directory.
        """
        return self.path / "scripts"

    @property
    def grades_folder(self) -> Path:
        """
        The path to the scripts directory.
        """
        return existing_folder(self.path / "grades")

    @property
    def plagiarism_folder(self) -> Path:
        """
        The path to store plagiarism results.
        """
        return existing_folder(self.path / "plagiarism")

    @property
    def github_classroom_ids(self) -> list[gh.GithubClassroomId]:
        """
        The list of GitHub Classroom IDs from the submissions URL.
        """
        # The official format for github classroom is `gh-classroom:<classroom_id>,<classroom_id>`.
        if self.submissions.startswith("gh-classroom:"):
            values = self.submissions.partition(":")[2].split(",")
            return [gh.github_classroom_id(value) for value in values]

        # We also accept copy and past from the github classroom UI, thus
        # gh classroom clone student-repos -a <classroom_id> is also valid.
        # We actually accept anything that starts with `gh classroom` and has a
        # full digits node.
        parts = self.submissions.strip().split()
        if parts[0:2] == ["gh", "classroom"]:
            id_parts = [*filter(str.isdigit, parts)]
            if len(id_parts) != 1:
                msg = "Invalid GitHub Classroom ID. Could not locate the classroom number."
                msg += "\nExpect the format `gh classroom clone student-repos -a <classroom_id>`."
                raise ValueError(msg)
            return [*map(gh.github_classroom_id, id_parts)]

        return []

    #
    # Private properties
    #
    @cached_property
    def _meta(self) -> PathMetadata:
        return PathMetadata(self.cache_folder)

    @property
    def _podman_image(self) -> str | None:
        return self._meta.get("podman-image", None)

    @_podman_image.setter
    def _podman_image(self, value: str) -> None:
        self._meta["podman-image"] = value

    def _notify_step(self, step: str, error: bool = False) -> None:
        """
        Notify that the activity was successfully initialized.
        """
        if error:
            print(f" ❌ {step}", flush=True)
        else:
            print(f" ✅ {step}", flush=True)

    #
    # Pipeline functions
    #
    def run(self, **kwargs):
        trio.run(partial(self.run_async, **kwargs))

    async def run_async(self, **kwargs) -> None:
        with rich.progress.Progress() as progress:
            # Progress meters
            init = progress.add_task("Fetch template", total=1)
            fetch = progress.add_task("Fetch submissions", total=1)

            if not kwargs.get("skip_init", False):
                await self.init_pipeline()
            progress.advance(init)

            repos = await self.fetch_submissions()
            progress.advance(fetch)

            prepare = progress.add_task("Clean repos", total=len(repos))
            autograde = progress.add_task("Autograde", total=len(repos))
            manual = progress.add_task("Fetch manual responses", total=len(repos))
            plagiarism = progress.add_task("Find plagiarism", total=len(repos))

            # Clean repos and autograde
            async def run(repo: Repo, acc: dict[T.RepoId, T.GradeSet]):
                if not kwargs.get("skip_clean", False):
                    result = await self.prepare_submission(repo)
                    is_success = isinstance(result, Success)
                else:
                    is_success = True
                progress.advance(prepare)

                if is_success and not kwargs.get("skip_autograde", False):
                    acc[repo.id] = await self.autograde_submission(repo)
                progress.advance(autograde)

            acc: dict[T.RepoId, T.GradeSet] = {}
            async with trio.open_nursery() as nursery:
                for repo in repos:
                    nursery.start_soon(run, repo, acc, name=repo.id)
            autogrades = transpose_dicts(acc)
            del acc

            # Manual responses
            responses = await self.manual_responses(repos)
            manual_grades = await self.collect_manual_grades(responses)
            progress.advance(manual)

        # Manual grading
        self.interactive_grading(responses)
        # responses = self.manual_responses(clean_repos)
        # manual_grades = self.collect_manual_grades(responses)

        # # Grades
        # grades = merge_dicts_of_dicts(autogrades, manual_grades)
        # df = collect_grades(
        #     grades,
        #     include=self.questions.include,
        #     exclude=self.questions.exclude,
        #     weights=self.questions.merged_weights(),
        #     merge=self.questions.merge,
        #     save=self.grades_folder / "grades.csv",
        # )

        # print(df.describe().T)

    async def init_pipeline(self):
        """
        Start the pipeline by setting up the template directory and the
        sandbox environment, if present.
        """
        # Validate/init the template folder
        if not self.template_folder.exists():
            raise NotImplementedError("cannot fetch template")

        if self.sandbox == "podman":
            container_file = self._podman_container_file()
            image = await tasks.podman.build(
                cwd=self.path,
                options=AnnotatedPath(self.cache_folder),
                tag=self._podman_tag(),
                container_file=container_file,
            )
            self._podman_image = image

            if not container_file.exists():
                msg = f"Containerfile {container_file} does not exist."
                raise ConfigurationError(msg)

        elif self.sandbox == "none":
            pass
        else:
            raise ValueError(f"Invalid sandbox type: {self.sandbox!r}")

    async def fetch_submissions(self, force: bool = False) -> list[Repo]:
        """
        Fetch submissions from the submissions sources.

        For now, it only supports GitHub Classroom.
        """
        if ids := self.github_classroom_ids:
            submissions = await gh.fetch_github_classroom_submissions(
                ids=ids,
                submissions_folder=self.submissions_folder,
                cache_folder=self.cache_folder,
                method="update" if force else "fast",
            )
            return submissions
        raise NotImplementedError

    async def prepare_submission(
        self,
        repo: Repo,
        remove: bool = True,
        protect: bool = True,
        dedup: bool = True,
        validate: bool = True,
        force: bool = False,
    ) -> T.RepoResult[Repo]:
        """
        Prepare a submission for grading.

        It cleans up the repository, validates it, and prepares it for grading.
        If everything goes correctly, returns a Success(repo), otherwise, return
        a Failure case with the error.
        """
        require_patterns = [
            *self.files.expect_patterns(),
            *self.files.require_patterns(),
        ]
        if remove:
            await tasks.clean.remove(
                repo,
                exclude=self.files.exclude_patterns(),
                keep=require_patterns,
                force=force,
            )
        if protect:
            await tasks.clean.protect(
                repo,
                template=self.template_folder,
                protect=self.files.protect_patterns(),
                keep=require_patterns,
                force=force,
            )
        if dedup:
            await tasks.clean.dedup(
                repo,
                template=self.template_folder,
                force=force,
                no_dedup=[
                    *self.files.exclude_patterns(),
                    *self.files.require_patterns(),
                ],
            )
        if validate:
            shutil.rmtree(self.submission_errors_folder, ignore_errors=True)
            return await tasks.clean.validate(
                repo,
                force=force,
                link_to=self.submission_errors_folder,
                required=self.files.require_patterns(),
            )
        return Success(repo)

    #
    # Auto grading functions
    #
    async def collect_autogrades(
        self, repos: Iterable[Repo] | None = None
    ) -> T.RepoGradeSet:
        """
        Collect autogrades from the repositories.
        """
        if repos is None:
            repos = [repo async for repo in self.repositories()]

        autogrades: T.RepoGradeSet = defaultdict(dict)
        for repo in repos:
            grades = await self.autograde_submission(repo)
            for question_id, grade in grades.items():
                autogrades[question_id][repo.id] = grade
        return autogrades

    async def autograde_submission(self, repo: Repo, force: bool = False) -> T.GradeSet:
        """
        Autograde submission in the given repository.
        """
        autograders: dict[str, T.GradeSet | None] = {}
        for name in self.autograde or []:
            if name == "podman":
                autograders[name] = await self._podman_autograde_submission(
                    repo,
                    force=force,
                )
            elif is_entry_point_name(name):
                autograders[name] = await self._script_autograde_submission(
                    repo,
                    name,
                    force=force,
                )
            else:
                raise ValueError(f"Invalid autograder name: {name!r}")

        given_grades = {k: v for k, v in autograders.items() if v is not None}
        try:
            grades = merge_dicts(given_grades)
        except ValueError as e:
            raise RepoError(
                repo=repo,
                message=str(e),
            )
        else:
            return grades

    async def _podman_autograde_submission(
        self,
        repo: Repo,
        force: bool = False,
    ) -> dict[T.QuestionId, T.Grade] | None:
        """
        Autograde submission in the given repository using Podman.
        """
        image = self._podman_image
        if image is None:
            msg = "Could not determine the Podman image to run.\n"
            msg += f"Maybe it can be fixed by running `maestro pipeline {self.slug}`?"
            raise ConfigurationError(msg)

        # FIXME: we are hard-coding the exit codes and the expect file
        # relevant to pytest here. Probably this should be configurable in the
        # future.
        exit_codes = [0, 1, 2, 3, 4, 5]
        report_path = repo.path / "report.json"
        is_successful = await tasks.podman.autograde(
            repo=repo,
            image=image,
            expect="report.json",
            exit_codes=exit_codes,
            force=force,
        )
        if not is_successful:
            # self.notify(f"Error auto grading {repo.id}", mode="error")
            return None

        try:
            grades = await tasks.pytest.parse_report(report_path)
        except json.JSONDecodeError:
            msg = f"Failed to parse the report.json file at {repo.id}."
            self.notify(msg, mode="error")
            return None

        if weights := self.questions.weights:
            generic = weights.get(T.QuestionId("*"), T.Grade(1.0))
            for k, v in grades.items():
                grades[k] = T.Grade(v) * weights.get(T.QuestionId(k), generic)

        return {T.QuestionId(k): T.Grade(v) for k, v in grades.items()}

    def _podman_container_file(self):
        return self.path / "Containerfile"

    def _podman_tag(self) -> str:
        classroom = self.classroom.slug
        slug = self.slug
        return f"maestro.{classroom}.{slug}"

    async def _script_autograde_submission(
        self,
        repo: Repo,
        name: str,
        force: bool = False,
    ) -> dict[T.QuestionId, T.Grade] | None:
        """
        Autograde submission in the given repository using a script.
        """
        autograder = load_entry_point(name, self.scripts_folder, check_callable=True)
        try:
            await trio.sleep(0)
            result = autograder(repo.path)
        except Exception as e:
            msg = f"{name}[{repo.id}], {e.__class__.__name__}: {e}"
            log.warning(msg)
            return None
        return result

    #
    # Manual grading
    #
    async def manual_responses(
        self, repos: Iterable[Repo] | None = None
    ) -> T.RepoResponseSet:
        """
        Collect all responses that require manual grading in all repositories.
        """
        if repos is None:
            repos = [repo async for repo in self.repositories()]

        out: T.RepoResponseSet = {}

        for repo in repos:
            data = self._manual_grade_response_from_submission(repo)
            for key, value in data.items():
                db = out.setdefault(key, {})
                db[repo.id] = value

        return out

    def interactive_grading(self, responses: T.RepoResponseSet):
        """
        Manually grade all responses collected from the repositories.
        """
        completed = self._has_completed_manual_grades(iter(responses))
        incomplete = set(responses) - {k for k, v in completed.items() if v}

        while incomplete:
            question_id = select_question(sorted(incomplete))
            if question_id is None:
                break
            else:
                incomplete.remove(question_id)

            data = responses[question_id]
            data = self.normalize_responses_before_grading(question_id, data)
            ui = self.interactive_grading_ui(question_id, data)

            _, finished = ui.run()
            if finished:
                self._complete_manual_grades(question_id)

    async def collect_manual_grades(
        self, responses: T.RepoResponseSet | None = None
    ) -> T.RepoGradeSet:
        """
        Collect manual grades from the responses.
        """
        from ..cli.sort_grading import grades

        if responses is None:
            responses = await self.manual_responses()

        result = {}
        for question_id, data in responses.items():
            kwargs = self._sort_grading_kwargs(question_id, data)
            kwargs.pop("syntax", None)
            kwargs.pop("show_id", None)
            result[question_id] = cast(dict[T.RepoId, T.Grade], grades(**kwargs))
        return result

    def interactive_grading_ui(
        self, id: T.QuestionId, responses: dict[T.RepoId, T.ResponseItem]
    ) -> SortGrading[T.RepoId, T.Grade]:
        """
        Create an interactive grading UI for the given question ID and responses.
        """
        from ..cli.sort_grading import SortGrading

        return SortGrading(**self._sort_grading_kwargs(id, responses))

    def _sort_grading_kwargs(
        self, id: T.QuestionId, responses: dict[T.RepoId, T.ResponseItem]
    ):
        """
        Can be used as arguments for sort_grades.SortGrading and sort_grades.grades
        callables.
        """
        options = self.manual_grading_options.get(id, {"syntax": "md"})
        return dict(
            responses=cast(dict[T.RepoId, str], responses),
            save_file=self.grades_folder / f"question-{id}.json",
            syntax=options.get("syntax", "text"),
            show_id=options.get("show_id", True),
            grade_levels=options.get("grade_levels", None),
        )

    # TODO: make async
    def _manual_grade_response_from_submission(
        self, repo: Repo
    ) -> dict[T.QuestionId, T.ResponseItem]:
        """
        Collect responses for all questions that require manual grading in a
        given repository.
        """
        response: dict[T.QuestionId, T.ResponseItem] = {}
        errors: dict[Path, str] = {}

        for question in self.questions.manual:
            file = repo.path / question.file
            if not file.exists():
                msg = f"response file {question.file} not found in {repo.id}"
                errors[question.file] = msg
                repo.meta.log_append("manual-responses", msg)
                continue

            # Question defines a parser
            elif question.parse:
                parser = load_entry_point(
                    question.parse, self.scripts_folder, check_callable=True
                )
                try:
                    result = parser(file)
                except ValueError as e:
                    msg = f"Failed to parse question from {str(question.file)!r}: {e}"
                    repo.meta.log_append("manual-responses", msg)
                    continue

                if isinstance(result, str):
                    key = question.name or question.file.stem
                    response[T.QuestionId(key)] = T.ResponseItem(result.strip())

                elif isinstance(result, dict):
                    response.update(result)

                else:
                    msg = f"ERROR: Invalid parser result for {str(question.file)!r}: {type(result)}"
                    repo.meta.log_append("manual-responses", msg)
                    raise ValueError(msg)

            # Question defines a name. The file content is the response.
            elif question.name:
                data = file.read_text(encoding="utf-8")
                key = question.name
                response[T.QuestionId(key)] = data.strip()

            # Question only specifies a file. We assume the name is equal to the
            # file name without the extension.
            else:
                key = question.file.stem
                data = file.read_text(encoding="utf-8")
                response[T.QuestionId(key)] = data.strip()

        return response

    def _has_completed_manual_grades(
        self, ids: Iterator[T.QuestionId]
    ) -> dict[T.QuestionId, bool]:
        """
        Return a mapping telling each question id if they are finishe with
        manual grading or not.
        """
        data, _ = self._complete_manual_grades_data()
        return {id: data.get(id, False) for id in ids}

    def _complete_manual_grades(self, id: T.QuestionId, value: bool = True) -> None:
        """
        Mark the manual grading for the given question ID as completed.

        If value is False, mark it as explicitly not completed.
        """
        data, path = self._complete_manual_grades_data()
        data[id] = value

        with path.open("w", encoding="utf-8") as fd:
            json.dump(data, fd, indent=2)

    def _complete_manual_grades_data(self) -> tuple[dict[T.QuestionId, bool], Path]:
        file = self.grades_folder / "completed.json"
        if not file.exists():
            return {}, file

        with file.open("r", encoding="utf-8") as fd:
            try:
                data = json.load(fd)
                assert isinstance(data, dict)
                return data, file
            except (ValueError, TypeError, AssertionError) as e:
                raise ConfigurationError(f"Invalid completed manual grades file: {e}")

    def normalize_responses_before_grading(
        self, id: T.QuestionId, responses: dict[T.RepoId, T.ResponseItem]
    ) -> dict[T.RepoId, T.ResponseItem]:
        """
        Normalize the responses before grading.
        """
        import spacy

        if self.answer_key not in responses:
            return responses

        nlp = spacy.load("pt_core_news_md")
        reference = nlp(responses[self.answer_key])
        similarities = {}

        # Show progressbar in interactive mode
        if self.interactive:
            response_items = rich.progress.track(responses.items(), "Sorting responses")
        else:
            response_items = responses.items()

        # Compute similarity for each response
        for repo_id, response in response_items:
            if not response:
                similarities[repo_id] = 0.0
            else:
                doc = nlp(response)
                similarity = doc.similarity(reference)
                similarities[repo_id] = similarity

        sorted_items = sorted(
            responses.items(),
            key=lambda pair: similarities[pair[0]],
            reverse=True,
        )

        return dict(sorted_items)

    #
    # Plagiarism detection and actions
    #
    async def plagiarism_pipeline(self, force: bool = False):
        """
        Run the plagiarism detection pipeline for the activity.
        """
        type Duplicate = DuplicateFile | DuplicateResponse
        type Policy = PlagiarismPolicyItem

        duplicates = await self.detect_plagiarism(force=force)

        # Match each duplicate with the corresponding policy
        no_policy: list[Duplicate] = []
        policies: dict[Duplicate, Policy] = {}

        for duplicate in duplicates:
            policy = self.plagiarism.match(duplicate)
            if policy is None:
                no_policy.append(duplicate)
                continue
            elif policy.ignore:
                continue
            policies[duplicate] = policy

        # Interactive mode for resolving missing policies
        if no_policy:
            for duplicate in no_policy:
                duplicate.print_report()
                input("Press Enter to continue...")
            raise NotImplementedError

        # Resolve policies into grade modifiers
        type Totals = dict[T.RepoId, float]
        type Grades = dict[T.QuestionId, dict[T.RepoId, float]]

        total_mul: Totals = defaultdict(lambda: 1.0)
        total_add: Totals = defaultdict(lambda: 0.0)
        grades_mul: Grades = defaultdict(lambda: defaultdict(lambda: 1.0))
        grades_add: Grades = defaultdict(lambda: defaultdict(lambda: 0.0))
        observations: dict[T.RepoId, set[str]] = defaultdict(set)

        for duplicate, policy in policies.items():
            ids = duplicate.ids
            action = policy.action
            file = getattr(duplicate, "file", None)
            if isinstance(duplicate, DuplicateFile):
                questions = getattr(policy, "questions", [])
            else:
                questions = [duplicate.question_id]

            # Save data to the "observations" column
            for repo_id in ids:
                if file:
                    observations[repo_id].add(str(file))
                else:
                    for question_id in questions:
                        observations[repo_id].add(question_id)

            # Save modifiers to grade totals
            if not questions:
                if action.is_multiplicative:
                    factor = action.multiplier_for(duplicate)
                    for repo_id in ids:
                        total_mul[repo_id] *= factor
                elif action.is_additive:
                    factor = action.increment_for(duplicate)
                    for repo_id in ids:
                        total_add[repo_id] += factor
                continue

            # Save modifiers to individual grades for questions
            for question_id in questions:
                if action.is_multiplicative:
                    factor = action.multiplier_for(duplicate)
                    for repo_id in ids:
                        grades_mul[question_id][repo_id] *= factor
                elif action.is_additive:
                    factor = action.increment_for(duplicate)
                    for repo_id in ids:
                        grades_add[question_id][repo_id] += factor

        return GradeModifiers(
            global_mul=total_mul,
            global_add=total_add,
            grades_mul=grades_mul,
            grades_add=grades_add,
            observations={k: "|".join(sorted(v)) for k, v in observations.items()},
        )

    async def detect_plagiarism(
        self,
        repos: list[Repo] | None = None,
        force: bool = False,
    ) -> list[DuplicateFile | DuplicateResponse]:
        """
        Detect plagiarism in the given repositories.
        """
        if repos is None:
            repos = [repo async for repo in self.repositories()]

        # Detect plagiarism on files
        path = trio.Path(self.plagiarism_folder / "duplicate-files.yaml")
        if force or not await path.exists():
            duplicate_files = await find_duplicate_files(
                repos,
                template_folder=self.template_folder,
                require=[*self.files.require_patterns()],
            )
            data = {
                "timestamp": datetime.datetime.now(),
                "duplicates": [
                    file.model_dump(mode="json") for file in duplicate_files
                ],
            }
            await path.write_text(yaml.dumps(data))
        else:
            async with await trio.open_file(path, "r") as fd:
                data = yaml.load(await fd.read())

            duplicate_files = Duplicates.model_validate(data["duplicates"]).root  # type: ignore

        # Detect plagiarism on responses
        path = trio.Path(self.plagiarism_folder / "duplicate-responses.yaml")
        if force or not await path.exists():
            responses = await self.manual_responses(repos)
            duplicate_responses = find_duplicate_responses(responses)
            data = {
                "timestamp": datetime.datetime.now(),
                "duplicates": [
                    file.model_dump(mode="json") for file in duplicate_responses
                ],
            }
            await path.write_text(yaml.dumps(data))
        else:
            async with await trio.open_file(path, "r") as fd:
                data = yaml.load(await fd.read())
            duplicate_responses = Duplicates.model_validate(data["duplicates"]).root  # type: ignore

        return [*duplicate_files, *duplicate_responses]

    #
    # Grading pipeline
    #
    async def grading_pipeline(self, skip_plagiarism: bool = False) -> pd.DataFrame:
        """
        Show the computed grades for the activity.
        """
        plagiarism_modifiers = (
            GradeModifiers() if skip_plagiarism else await self.plagiarism_pipeline()
        )
        manual_grades = await self.collect_manual_grades()
        autogrades = await self.collect_autogrades()
        raw_grades = merge_dicts_of_dicts(autogrades, manual_grades)
        raw_grades = plagiarism_modifiers.modify_grades(raw_grades)
        temporary_cols = set()

        # Handle includes and excludes
        if self.questions.include is not None:
            include = self.questions.include
            grades = {k: v for k, v in raw_grades.items() if k in include}
        else:
            grades = raw_grades
            for question_id in self.questions.exclude or ():
                grades.pop(question_id, None)

        # Merge columns
        new_columns: set[T.QuestionId] = set()
        if self.questions.merge:
            for merged_id, method in self.questions.merge.items():
                grades[merged_id] = method(grades)
                temporary_cols.update(method.items)
                new_columns.add(merged_id)

        # Remove temporary columns
        for col in temporary_cols:
            if col in grades:
                grades.pop(col)

        # Apply weights again to merged columns
        for question_id, value in self.questions.weights.items():
            if question_id not in new_columns:
                continue
            data = grades[question_id]
            for repo_id in data:
                data[repo_id] *= value  # type: ignore

        # Run plagiarism modifiers again for merged columns
        grades = plagiarism_modifiers.modify_grades(grades)
        df = pd.DataFrame(grades).astype(float).fillna(0.0)

        # FIXME: GAMBIRA DE ULTIMA HORA
        df = df[sorted(df.columns)]
        df["total"] = df.sum(axis=1)
        if self.total == "all-but-one":
            df["total"] = df["total"] - df.min(axis=1)

        df = df.round(2)
        df["obs.:"] = pd.Series(plagiarism_modifiers.observations)
        df["obs.:"] = df["obs.:"].fillna("")
        df = df.sort_index()

        # Remove duplicate rows and use the best grade
        totals = df["total"].to_dict()
        duplicates: dict[str, dict[str, float]] = defaultdict(dict)
        for full_id in df.index:
            if "@" not in full_id:
                continue
            repo_id, _, version = full_id.partition("@")
            duplicates[repo_id][version] = totals[full_id]

        rows = []
        rows_renamed = []
        for full_id in df.index:
            if "@" not in full_id:
                rows.append(full_id)
                rows_renamed.append(full_id)
            repo_id, _, version = full_id.partition("@")

            if repo_id not in duplicates:
                continue
            if duplicates[repo_id][version] == max(duplicates[repo_id].values()):
                rows.append(full_id)
                rows_renamed.append(repo_id)
                del duplicates[repo_id]

        df = df.loc[rows, :]
        df.index = rows_renamed  # type: ignore
        return df

    #
    # Transform pipeline
    #
    async def transform_pipeline(
        self, repos: list[Repo] | None = None
    ) -> dict[tuple[T.RepoId, str], Any]:
        """
        Run the transformation pipeline for the activity.
        """
        if repos is None:
            repos = [repo async for repo in self.repositories()]

        if not self.transforms:
            self.notify("No transforms to run", mode="warning")
            return {}

        bad_repos = set()
        for repo in repos:
            if repo.meta.get("missing-files", []):
                bad_repos.add(repo.id)
            elif repo.meta.get("podman-autograde", "").endswith(":failed"):
                bad_repos.add(repo.id)

        # Run each transform on each repository
        results = {}
        async with trio.open_nursery() as nursery:
            for transform in self.transforms:
                action = load_entry_point(
                    transform.action,
                    self.scripts_folder,
                    check_callable=True,
                )

                async def runner(repo, transform, action: Callable):
                    try:
                        result = action(repo.path, repo.meta)
                    except Exception as e:
                        msg = f"Bad transform {transform.name!r} on {repo.id},\n"
                        msg += f"    {e.__class__.__name__}: {e}"
                        log.warning(msg)
                        repo.meta.log_append("transforms", msg)
                    else:
                        if result is not None:
                            results[(repo.id, transform.name)] = result

                for repo in repos:
                    if transform.only == "err" and repo.id not in bad_repos:
                        continue
                    elif transform.only == "ok" and repo.id in bad_repos:
                        continue

                    nursery.start_soon(
                        runner,
                        repo,
                        transform,
                        action,
                        name=f"{transform.name}({repo.id})",
                    )

        return results

    #
    # Generic methods
    #
    async def repositories(self) -> AsyncGenerator[Repo, None]:
        """
        Iterate over all repositories in the activity.
        """
        async for path in self.repository_paths():
            yield Repo(path)

    async def repository_paths(self) -> AsyncGenerator[Path, None]:
        """
        Iterate over all repository paths in the activity.
        """
        submissions = trio.Path(self.path / "submissions")
        if not await submissions.exists():
            return
        elif not await submissions.is_dir():
            msg = f"Submissions path {submissions} is not a directory."
            raise ConfigurationError(msg)

        for path in await submissions.iterdir():
            if not await path.is_dir():
                continue
            yield Path(path)

    #
    # Cleaning functions
    #
    async def clean_template_links(
        self, repo_id: str | None = None, clean_invalid: bool = True
    ) -> None:
        """
        Clean symbolic links in repository that point to the template.

        Args:
            repo_id:
                The name of the repository to clean. If None, it cleans all
                repositories in the activity.
            clean_invalid:
                If True, it will also remove links that point to invalid paths.
        """
        if repo_id is None:
            async for path in self.repository_paths():
                await self.clean_template_links(path.name)
            return

        path = self.submissions_folder / repo_id
        if not path.exists() or not path.is_dir():
            raise ValueError(f"Invalid repository: {repo_id}.")

        for sub_path in path.glob("**/*"):
            if not sub_path.is_symlink():
                continue

            if clean_invalid:
                try:
                    sub_path.resolve(strict=True)
                except (FileNotFoundError, PermissionError):
                    sub_path.unlink()
                    continue

            if sub_path.is_relative_to(self.template_folder):
                sub_path.unlink()

        repo = Repo(path)
        del repo.meta["dedup-files"]

    async def clean_annotations(self, repo_id: str | None = None) -> None:
        """
        Clean all annotations for the repository.

        Args:
            repo_id:
                The name of the repository to clean. If None, it cleans all
                repositories in the activity.
        """
        if repo_id is None:
            async for path in self.repository_paths():
                await self.clean_annotations(path.name)
        else:
            repo = self.get_repo(repo_id)
            repo.meta.clear()

    async def clean_files(self, repo_id: str | None = None):
        """
        Clean all files in the repository according to the configuration.
        """
        clean = partial(self.prepare_submission, force=True, validate=False)

        if repo_id is not None:
            return trio.run(clean, Repo(repo_id))

        async with trio.open_nursery() as nursery:
            async for path in self.repository_paths():
                repo = Repo(path)
                nursery.start_soon(clean, repo, name=repo.id)

    def get_repo(self, repo_id: str) -> Repo:
        """
        Get a repository by its ID.

        Args:
            repo_id:
                The name of the repository to get.
        """
        path = self.submissions_folder / repo_id
        if not path.exists() or not path.is_dir():
            raise ValueError(f"Invalid repository: {repo_id}.")

        return Repo(path)


#
# Utility classes
#
@dataclass
class GradeModifiers:
    global_mul: dict[T.RepoId, float] = field(default_factory=dict)
    global_add: dict[T.RepoId, float] = field(default_factory=dict)
    grades_mul: dict[T.QuestionId, dict[T.RepoId, float]] = field(default_factory=dict)
    grades_add: dict[T.QuestionId, dict[T.RepoId, float]] = field(default_factory=dict)
    observations: dict[T.RepoId, str] = field(default_factory=dict)

    def modify_grades(self, grades: T.RepoGradeSet) -> T.RepoGradeSet:
        """
        Modify the grades according to the collected modifiers.

        This method does not include the global modifers.
        """
        result = copy.deepcopy(grades)
        for question_id, data in result.items():
            mul = self.grades_mul.get(question_id, {})
            add = self.grades_add.get(question_id, {})

            for repo_id, increment in add.items():
                if repo_id in data:
                    data[repo_id] += increment  # type: ignore
            for repo_id, factor in mul.items():
                if repo_id in data:
                    data[repo_id] *= factor  # type: ignore
        return result

    def modify_totals(self, totals: dict[T.RepoId, T.Grade]) -> dict[T.RepoId, T.Grade]:
        """
        Modify the totals according to the collected modifiers.

        This method does not include the global modifers.
        """
        add = self.global_add.get
        mul = self.global_mul.get
        result: dict[T.RepoId, T.Grade] = {}
        for repo_id, total in totals.items():
            result[repo_id] = (total + add(repo_id, 0.0)) * mul(repo_id, 1.0)  # type: ignore
        return result


#
# Utility functions
#
def collect_grades(
    data: T.RepoGradeSet,
    include: Iterable[T.QuestionId] | None = None,
    exclude: Iterable[T.QuestionId] | None = None,
    merge: dict[T.QuestionId, Aggregator] | None = None,
    weights: dict[T.QuestionId, T.Grade] | None = None,
    save: Path | None = None,
) -> pd.DataFrame:
    """
    Collect grades from the grading results.

    Args:
        grades:
            A list of grading results.
        include:
            A list of questions to include in the DataFrame.
        exclude:
            A list of questions to exclude from the DataFrame.
        merge:
            A dictionary mapping question names to a list of question names to
            merge into a single column.
        weights:
            A dictionary mapping question names to their weights. If not given,
            it uses the default weight of 1.0 for each question.
        save:
            If given, it saves the resulting DataFrame to a CSV file at the
            specified path. If not given, it returns the DataFrame without saving.
    """
    kwargs = {"merge": merge, "weights": weights, "save": save}

    if include is not None and exclude is not None:
        raise ValueError("Cannot specify both include and exclude.")
    elif exclude is not None:
        exclude_set = set(exclude)
        data = {k: v for k, v in data.items() if k not in exclude_set}
        return collect_grades(data, **kwargs)  # type: ignore
    elif include is not None:
        include_set = set(include)
        data = {k: v for k, v in data.items() if k in include_set}
        return collect_grades(data, **kwargs)  # type: ignore

    df = pd.DataFrame(data).fillna(0.0)
    if merge is not None:
        columns = df.columns
        try:
            df = merge_columns(df, merge, inplace=True)
        except KeyError as e:
            msg = f"Could not find columns: {e}\n"
            msg += f"Dataframe only contain columns {columns}.\n"
            msg += "Check the questions.merge configuration."
            raise ConfigurationError(msg)

    df = df[sorted(df.columns)]
    df["total"] = df.sum(axis=1)

    # Save dataframe to a file, if requested
    if save is not None:
        save.parent.mkdir(parents=True, exist_ok=True)
        df.round(3).to_csv(save, index_label="id")

    return df


def merge_columns(
    df: pd.DataFrame,
    merge: dict[T.QuestionId, Aggregator],
    inplace: bool = False,
) -> pd.DataFrame:
    if not inplace:
        df = df.copy()

    for key, by in (merge or {}).items():
        zero = T.Grade(0)
        match (by.method, by.implicit_zero):
            case ("sum", _):
                df[key] = df[by.items].fillna(zero).sum(axis=1)
            case ("average", fill_zero):
                columns = df[by.items]
                if fill_zero:
                    columns = columns.fillna(zero)
                df[key] = columns.mean(axis=1)
            case ("max", _):
                df[key] = df[by.items].max(axis=1).fillna(zero)
            case ("min", _):
                df[key] = df[by.items].min(axis=1).fillna(zero)
            case ("geometric", _):
                columns = df[by.items]
                if fill_zero:
                    columns = columns.fillna(zero)
                df[key] = (columns.product(axis=1) / columns.count(axis=1)).fillna(zero)
            case _:
                raise ValueError(f"Invalid merge method: {by.method!r}")

        for merged in by.items:
            df.pop(merged)
    return df


def select_question(questions: list[T.QuestionId]) -> T.QuestionId | None:
    """
    Ask the user which question they want to grade next.
    """

    rich.print("\nWhich question do you want to grade?")
    for i, question in enumerate(questions, start=1):
        rich.print(f"  [b green]{i}.[/] [b white]{question}[/]")
    rich.print("  [b red]0.[/] [red]Skip grading (default)[/]")

    index = rich.prompt.IntPrompt.ask(
        prompt="Select a number",
        choices=list(map(str, range(len(questions) + 1))),
        default=0,
        show_choices=False,
        show_default=False,
    )
    if index == 0:
        return None
    return questions[index - 1]
