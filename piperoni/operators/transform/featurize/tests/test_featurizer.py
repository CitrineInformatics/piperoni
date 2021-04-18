import os
import pytest
import pandas

from pandas import DataFrame, read_csv

import piperoni as hep

from piperoni.operators.transform.featurize.featurizer import (
    CustomFeaturizer,
)


"""
This module implements tests for the CustomFeaturizer.
"""


def multiply_column_by_two(df: DataFrame, col_name: str):
    """Test function for CustomFeaturizer

    Parameters
    ----------
    df: DataFrame
        The input data.
    col_name: str
        The column to multiply by two.

    Returns
    -------
    DataFrame
        The selected column, but multiplied by two.
    """
    result = DataFrame()
    result["{}x2".format(col_name)] = df[col_name].values * 2.0
    return result


class TestCustomFeaturizer:
    """Tests funtionality of CustomFeaturizer."""

    @pytest.fixture(scope="class")
    def transformer(self):
        return CustomFeaturizer(multiply_column_by_two, col_name="Band gap")

    @pytest.fixture(scope="class")
    def input_data(self):
        root_dir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(
            root_dir,
            "..",
            "..",
            "..",
            "..",
            "..",
            "test_files",
            "Strehlow and Cook.csv",
        )
        return read_csv(path)

    def test_transform(self, transformer, input_data):
        """Tests that an additional column is added to the input data."""
        result = transformer(input_data)

        # validate that extra column in result exists
        assert "Band gapx2" in result.columns.values

        # validate that original data was unaltered
        original_columns = input_data.columns.values
        copied_data = result[original_columns]
        for i, j in zip(
            copied_data.values.flatten(), input_data.values.flatten()
        ):
            if pandas.isna(i) and pandas.isna(j):
                assert True
            else:
                assert i == j
