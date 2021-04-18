"""Implements value transformers, which modify existing data."""

from typing import Callable, List
from copy import deepcopy

from pandas import DataFrame
from piperoni.operators.base import BaseOperator


class Normalizer(BaseOperator):
    """Shift the mean within a column.

    Parameters
    ----------
    columns: List[str]
        Specifies which columns (by their headers) to normalize.
    delta_mus: List[float]
        Values by which to shift the means of each column. The delta will be
        added to each value in that column.
    """

    def __init__(self, columns: List[str], delta_mus: List[float]):
        self.columns = columns
        self.delta_mus = delta_mus

    def transform(self, df: DataFrame) -> DataFrame:
        """Normalize columns of the data-frame.

        Parameters
        ----------
        df: DataFrame
            The input data to normalize.

        Returns
        -------
        DataFrame
            The normalized data.
        """
        df = deepcopy(df)  # don't overwrite input df

        for i, column in enumerate(self.columns):
            df[column] = df[column].values + self.delta_mus[i]

        return df
