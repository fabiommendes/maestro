from collections import defaultdict
import json
from numbers import Number
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Sequence, Tuple, Union, TYPE_CHECKING

import pandas as pd
from thefuzz import process

if TYPE_CHECKING:
    from .models import Course

REGISTRY: Dict[str, Callable[[pd.DataFrame, str, Path], pd.DataFrame]] = {}


def register(ext: str):
    """
    Register transformer by extension.
    """

    def decorator(fn):
        REGISTRY[ext] = fn
        return fn

    return decorator


@register("transform")
def transform_maestro(
    repo: "Course", data: pd.DataFrame, src: str, script: Path
) -> pd.DataFrame:
    """
    Transform dataframe using a maestro recipe.
    """
    import fred

    recipe = fred.loads(src)
    if not isinstance(recipe, list):
        recipe = [recipe]

    for step in recipe:
        if not isinstance(step, fred.Tag):
            raise ValueError("invalid step: expect a tag")
        print(step, step.tag)
        data = transform_maestro_step(
            repo, data, cmd=step.tag, options=step.value, script=script
        )

    return data


def transform_maestro_step(
    repo: "Course", data: pd.DataFrame, cmd: str, options: Dict[str, Any], script: Path
):
    """
    Apply a single step in a transform.
    """
    if cmd == "reindex":
        source = options["from"]
        return reindex_data(repo, data, source)
    elif cmd == "competencies":
        # source = repo.path.joinpath(options["from"])
        # competencies = json.loads(source.read_text())
        return reduce_competencies(repo, data)
    elif cmd == "clean":
        columns = [col for col in data.columns if not col.endswith("?")]
        return data[columns]
    else:
        raise ValueError(f"invalid command: {cmd}")


@register("py")
def transform_python(
    course: "Course", data: pd.DataFrame, src: str, script: Path
) -> pd.DataFrame:
    """
    Transform dataframe using Python script.
    """
    ns = {}
    code = compile(src, script, "exec")
    exec(code, ns)
    try:
        fn = ns["transform"]
        if not callable(fn):
            raise ValueError
    except (KeyError, ValueError):
        raise ValueError(
            f"python script at {course.path} do not define a transform(df) -> df function."
        )
    else:
        result = fn(data)

        if isinstance(result, pd.DataFrame):
            return result

        typ = type(result).__name__
        msg = f"transform function should return a dataframe, not {typ}"
        raise TypeError(msg)


@register("tt")
def transform_tt(
    repo: "Course", data: pd.DataFrame, src: str, script: Path
) -> pd.DataFrame:
    """
    Transform dataframe using a tt transformation.
    """
    import tt

    return data.tt(src)


#
# Auxiliary transformations
#
COLUMN_ALIASES = {
    "checkio": "checkio_id",
    "github": "github_id",
}


def reindex_data(repo: "Course", data: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Transform data that do not use the main id as index.

    This is useful, for instance, to use a table indexed by E-mail,
    Github username or some other unique identifier which does not coincide with
    the main id.
    """
    column = column.replace(" ", "_").lower()
    column = COLUMN_ALIASES.get(column, column)

    if column in ("checkio_id", "github_id", "email"):
        try:
            col_data = data[column]
        except KeyError:
            col_data = pd.Series(data.index, name=column)

        id_map = {getattr(st, column): id for id, st in repo.classroom.students.items()}
        id_map.pop(None, None)
        fetch_id = robust_match(id_map, col_data)
        data["id"] = col_data.apply(fetch_id).values
        data.dropna(axis=0, subset=["id"], inplace=True)
        data = data.set_index("id")
    else:
        raise ValueError(f"cannot reindex from {column}")

    return data


def reduce_competencies(
    repo: "Course",
    data: pd.DataFrame,
    mark_unkown: Union[bool, Callable[[str, Any], Tuple[str, Any]]] = True,
) -> pd.DataFrame:
    """
    Reduce a table using a competence mapping.

    Args:
        data:
            The input dataframe
        mark_unknown:

    """
    reduced = []
    exercises = repo.classroom.exercises
    competence_map = {id: ex.competencies for id, ex in exercises.items()}

    for ref, row in data.iterrows():
        row = {k: v for k, v in row.to_dict().items() if v != 0}
        new = defaultdict(int, {"id": ref})
        for k, v in row.items():
            try:
                assigned_points = competence_map[k].items()
            except KeyError:
                if mark_unkown is True:
                    k = f"{k}?"
                elif mark_unkown:
                    k, v = mark_unkown(k, v)
                new[k] = v
            else:
                for competence_id, points in assigned_points:
                    new[competence_id] += 0 if not v else points

        reduced.append(new)

    return pd.DataFrame.from_records(reduced).set_index("id")


def robust_match(
    exact: Dict[str, str], approx: Sequence[str] = ()
) -> Callable[[str], Optional[str]]:
    """
    Return a match function that accepts fuzzy matching
    """
    db = dict(exact)
    correct_values = list(db)

    def fn(key):
        try:
            return db[key]
        except KeyError:
            exact, score = process.extractOne(key, correct_values)
            if score >= 90:
                print(f"Fuzzy match: {key} => {exact} score: {score:d}%")
                return exact
            else:
                print(f"Bad match: {key}")
                return None

    return fn
