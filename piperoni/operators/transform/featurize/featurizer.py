"""Implements featurizers, which add columns to existing data."""

from typing import Callable

from pandas import DataFrame, concat
from piperoni.operators.base import BaseOperator


class CustomFeaturizer(BaseOperator):
    """A customizable featurizer for pandas DataFrames.

    Parameters
    ----------
    func: Callable
        The callable used to generate new DataFrame columns. The callable
        should accept a DataFrame as the first argument followed by any *args
        and **kwargs. It should return additional columns as a DataFrame.
    kwargs: keyword arguments
        Keyword arguments passed to func.

    Notes
    -----
    This operator will concatenate data returned by func with the input data.
    """

    def __init__(self, func: Callable, **kwargs: dict):
        self.func = func
        self.kwargs = kwargs

    def transform(self, df: DataFrame) -> DataFrame:
        """Apply the custom featurizer to input data.

        Parameters
        ----------
        df: DataFrame
            The input data-frame to featurize.

        Returns
        -------
        DataFrame
            Concatenation of the input DataFrame and the additional columns.
        """
        new_columns = self.func(df, **self.kwargs)
        return concat([df, new_columns], axis=1)
