import pandas as pd
import warnings
import copy

from piperoni.operators.passthrough_operator import PassthroughOperator
from piperoni.utils import compare_dataframes


"""
This module implements the DataFrame comparsion tool.

DataFrame comparsion tool checks a DF against a reference to find any differences in rows and columns
"""


class CompareOperator(PassthroughOperator):
    """
    Compares an incoming dataset to a reference dataset.
    First checks if columns are consistent.
    Then, checks whether if all entries in the reference are present in the incoming dataset
    by comparing the unique entries.
    Then, checks that the values are consistent from the input to the reference for all
    columns that are present in both.
    Finally, reports newly added rows.

    Parameters
    ----------
    reference: pd.DataFrame
        Dataset to be used as the reference
    unique_id_col: str
        Name of the column that can be used as a unique identifier. This
        allows comparison of rows for any changes between the input and
        the reference.
    ignore_cols: str or None, optional
        Name of column(s) to ignore in analysis
    """

    def __init__(
        self, reference: pd.DataFrame, unique_id_col: str, ignore_cols=None
    ):
        self.reference = copy.deepcopy(reference)
        self.unique_id_col = unique_id_col
        self.ignore_cols = ignore_cols

    def test_equals(self, input_, output_) -> bool:
        return input_.equals(output_)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:

        compare_dataframes(
            df, self.reference, self.unique_id_col, self.ignore_cols
        )

        return df
