from collections import defaultdict
from logging import getLogger
from pathlib import Path
from typing import Dict, MutableMapping, TYPE_CHECKING

import sidekick.api as sk

from . import conf
from .pipeline_steps import (Step, Source, PRs, CSV, Pytest, SchoolId, Grader,
                             Competencies, IncludeFiles)

if TYPE_CHECKING:
    from pandas import DataFrame

log = getLogger('maestro')
SOURCE_ERROR = NotImplementedError('must initialize a source step')

__all__ = [
    'Pipeline', 'Step', 'Source', 'PRs', 'CSV', 'Pytest', 'SchoolId', 'Grader',
    'Competencies', 'IncludeFiles',
]


class Pipeline:
    """
    Defines a pipeline of steps.
    """
    source: Source = sk.lazy(sk.raising(SOURCE_ERROR))
    steps: Dict[str, Step]

    def __init__(self, path='.', raises=conf.DEBUG):
        self.path = Path(path)
        self.steps = {}
        self.raises = raises

    def add_step(self, name: str, step: Step):
        """
        Add a processing step into the pipeline.
        """
        if isinstance(step, Source):
            self.source = step
        else:
            self.steps.update((f'{name}.{k}', s) for k, s in step.pre_steps())

        self.steps[name] = step
        self.steps.update((f'{name}.{k}', s) for k, s in step.post_steps())
        return self

    def execute_pending(self):
        """
        Execute all pending tasks.
        """

        processing_nodes = list(self.source.collect())
        for ref, name, out in self._yield_pending(processing_nodes, set()):
            self.source.update_steps(ref, {name: out})

        return self

    def _yield_pending(self, nodes, blacklist):
        # Worker function for execute_pending()
        for step_name, step in self.steps.items():
            for ref, item in nodes:
                # Skip paused or processed nodes
                if ref in blacklist or step_name in item['steps']:
                    continue

                # Process and yield
                try:
                    res = step.process(item)
                except Exception as ex:
                    cls = type(ex).__name__
                    log.error(
                        f'[{step_name}:{ref}] {cls} raised when processing task: {ex}')
                    blacklist.add(ref)
                    if self.raises:
                        raise
                else:
                    item['steps'][step_name] = res
                    yield ref, step_name, res

    def collect(self, index: str, data: str, col_name: str = 'grade',
                fillna=None, save_to=None) -> 'DataFrame':
        """
        Return a dataframe from information collected in previous steps.

        Args:
            index:
                Step that collect school ids.
            data:
                Name of step that collect dataframe data. Data can be numeric or
                a dictionary of column names to values.
            col_name:
                Column name for numeric data.
            fillna:
                Fill-in value for missing data.
            save_to:
                If given, save dataframe to given path.
        """

        import pandas as pd
        dicts = []

        for ref, item in self.source.collect():
            row = {index: item['steps'][index]}
            content = item['steps'][data]
            if isinstance(content, MutableMapping):
                row.update(content)
            else:
                row[col_name] = content
            dicts.append(row)

        df = pd.DataFrame.from_records(dicts, index=index).fillna(fillna)
        if save_to is not None:
            df.to_csv(save_to)
        return df
