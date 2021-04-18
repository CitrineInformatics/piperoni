import os
import pytest
import pandas

from pandas import DataFrame, read_csv

import piperoni as hep

from piperoni.operators.transform.transform_value.value_transformers import (
    Normalizer,
)


"""
This module implements tests for the value transformers.
"""


@pytest.fixture(scope="module")
def input_data():
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


def multiply_column_by_two(df: DataFrame, col_name: str):
    """Test function for TransformOperator

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
    df[col_name] = df[col_name].values * 2.0
    return df


class TestNormalizer:
    """Tests funtionality of Normalizer."""

    @pytest.fixture(scope="class")
    def transformer(self):
        return Normalizer(
            columns=["Band gap", "uncertainty in band gap"],
            delta_mus=[-2.0, -3.0],
        )

    def test_transform(self, transformer, input_data):
        """Tests that the means of columns have been shifted."""
        result = transformer(input_data)

        # validate that values in both columns were shifted
        columns = ["Band gap", "uncertainty in band gap"]
        deltas = {"Band gap": -2.0, "uncertainty in band gap": -3}
        for column in columns:
            original_data = input_data[column]
            normalized_data = result[column]
            shift = deltas[column]
            for i, j in zip(original_data.values, normalized_data.values):
                if pandas.isna(i):
                    continue
                assert i + shift == j
