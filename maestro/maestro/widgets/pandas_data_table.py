from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd
from textual.reactive import reactive
from textual.widgets import DataTable

from ..logging import log

NOT_GIVEN = NotImplemented


class PandasDataTable(DataTable):
    """
    A DataTable that supports filtering of its data based on a reactive
    filter string.
    """

    filter = reactive("")
    _data_df: reactive[Data] = reactive(lambda: Data())
    _data_columns = reactive[list[str]](list)
    _filtered_data: reactive[Data] = reactive(lambda: Data(), repaint=True)

    @property
    def data(self) -> pd.DataFrame:
        return self._data_df.df

    @data.setter
    def data(self, value: pd.DataFrame) -> None:
        self._data_df = Data(value)

    def __init__(
        self, *args, data: pd.DataFrame = NOT_GIVEN, filter: str = "", **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.filter = filter
        if data is not NOT_GIVEN:
            self._data_df = Data(data)

    def watch__data_df(self, data: Data) -> None:
        """
        Update the filtered data when the main data changes.
        """
        id_column = str(data.df.index.name or "#")
        if len(data.df.columns) != 0:
            self._data_columns = [id_column, *data.df.columns]
            df = data.df.astype(str)
            self._filtered_data = Data(filter_dataframe(df, self.filter))

    def watch__data_columns(self, columns: list[str]) -> None:
        """
        Update the DataTable columns when the data columns change.
        """
        self.clear(columns=True)
        for col in columns:
            self.add_column(col, key=col)

    def watch_filter(self, filter_string: str) -> None:
        """
        Filter the data based on the filter string.
        """
        df = filter_dataframe(self._data_df.df, filter_string)
        self._filtered_data = Data(df)

    def watch__filtered_data(self, data: Data) -> None:
        """
        Update the DataTable rows when the filtered data changes.
        """
        self.clear()
        for index, row in data.df.iterrows():
            log.info(f"Adding row {index} to DataTable")
            log.info(f"Row data: {row}")
            self.add_row(index, *row, key=str(index))


def filter_dataframe(table: pd.DataFrame, substring: str) -> pd.DataFrame:
    """
    Apply a filter to the DataFrame based on the provided filter string.

    If the filter does not change any rows, the original DataFrame is returned.

    Args:
        table (pd.DataFrame):
            The DataFrame to filter. All columns must be strings.
        substring (str):
            The substring to filter by. Keep any lines that contain this
            substring in any column.
    """
    if not substring:
        return table

    def filter_row(row: pd.Series) -> bool:
        contains = row.str.contains(substring, case=False)
        return any(contains.values)

    mask = table.apply(filter_row, axis=1)
    return table if mask.all() else table[mask]


@dataclass
class Data:
    """
    Wraps a single DataFrame to provide a custom equality check and copy method.

    DataFrames do not work nicely as reactive properties in Textual because
    equality tests do not return booleans. We override to provide a identity check
    equality.
    """

    df: pd.DataFrame = field(default_factory=pd.DataFrame)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Data):
            return self.df is other.df
        return False

    def copy(self) -> pd.DataFrame:
        return self.df.copy()
