from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, ClassVar, Iterable, Literal

import pandas as pd

from .activity import BaseActivity, load_activity
from .errors import ConfigurationError
from .model import Model, parse_yaml


class Classroom(Model):
    """
    The configuration for a classroom repository.

    The classroom has the following structure:

    ```
    classroom/
    ├── activities/
    │   ├── activity1/
    │   │   ├── submissions/
    │   │   ├── template/
    │   │   ├── grades/
    │   │   └── activity.yaml
    │   └── .../
    ├── grades/
    ├── scripts/
    ├── maestro.yaml
    ├── roster.csv
    ├── grades.csv
    └── README.md
    ```

    The `activities`, `students`, and `grades` directories are used to store
    activities, student information, and grades respectively. The `maestro.yaml`
    file contains the configuration for the classroom, and the `README.md` file
    provides an overview of the classroom setup.
    """

    CONFIG_FILE: ClassVar = "maestro.yaml"
    path: Path
    type: Literal["maestro-classroom"] = "maestro-classroom"
    version: Literal["1.0"] = "1.0"

    @property
    def slug(self) -> str:
        """
        Return a slug for the classroom based on its path.
        """
        return self.path.name

    def prepare_folders(self) -> None:
        """
        Create all necessary directories and files for classroom repository.
        """
        root = self.path
        (root / "activities").mkdir(parents=True, exist_ok=True)
        (root / "students").mkdir(parents=True, exist_ok=True)
        (root / "grades").mkdir(parents=True, exist_ok=True)
        (root / "scripts").mkdir(parents=True, exist_ok=True)
        (root / "README.md").touch(exist_ok=True)

    #
    # Student management
    #
    @property
    def students_path(self) -> Path:
        return self.path / "students"

    def students_dataframe(self) -> pd.DataFrame:
        """
        Load the students data from the roster.csv file.
        """
        roster = self.path / "roster.csv"
        if not roster.exists():
            raise FileNotFoundError(f"Roster file not found: {roster}")
        df = pd.read_csv(roster, index_col="id", dtype=str)
        return df

    def students_hidden_columns(self) -> list[str]:
        """
        Return a list of columns that should be hidden in the students table.
        """
        return ["timestamp"]

    #
    # Activity management
    #
    @property
    def activities_path(self) -> Path:
        return self.path / "activities"

    def has_activities(self) -> bool:
        """
        Check if the classroom has any activities.
        """
        return any(
            (entry / "config.yaml").exists() for entry in self.activities_path.iterdir()
        )

    def iter_activities(
        self,
        on_error: Callable[[Any], None] = lambda _: None,
    ) -> Iterable[BaseActivity]:
        """
        List all activities in the classroom.
        """
        for base in self.activities_path.iterdir():
            slug = base.name
            try:
                yield self.get_activity(slug)
            except ValueError:
                msg = f"Skipping {slug}: config.yaml not found"
                on_error(msg)

    def get_activity(self, slug: str) -> BaseActivity:
        """
        Get an activity by its slug.
        """
        base = self.activities_path / slug
        config_path = base / BaseActivity.CONFIG_FILE

        if not base.exists() or not base.is_dir():
            raise ConfigurationError(f"Activity directory does not exist in {slug}")
        elif base.exists() and not config_path.exists():
            cfg = BaseActivity.CONFIG_FILE
            msg = f"Activity {slug} folder exists, but config file not found at {cfg}"
            raise ConfigurationError(msg)

        data: dict = parse_yaml(config_path)
        data.setdefault("path", base)
        return load_activity(data, self)

    def assignemnts_table(self) -> pd.DataFrame:
        """
        Load the activities data into a DataFrame.
        """
        columns = ["id", "name", "type"]
        activities = []
        for activity in self.iter_activities():
            row = {
                "id": activity.slug,
                "name": activity.name,
                "type": activity.type,
            }
            activities.append(row)
        return pd.DataFrame(activities, columns=columns).set_index("id")
