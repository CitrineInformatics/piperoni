import os
import pytest

import piperoni as etl

from glob import glob

from pandas import read_csv

from piperoni.operators.load.checkpoint import Checkpoint

"""
This module implements tests for the Checkpoint.
"""


class TestCheckpoint:
    """Tests funtionality of Checkpoint"""

    @pytest.fixture(scope="class")
    def dataframe(self):
        root_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(
            root_dir,
            "..",
            "..",
            "..",
            "..",
            "test_files",
            "Strehlow and Cook.csv",
        )
        # must use float precision due to read_csv bad rounding precision
        return read_csv(file_path, float_precision="round_trip")

    @pytest.fixture(scope="class")
    def default_instance(self):
        return Checkpoint("checkpoint_default.csv", index=False)

    @pytest.fixture(scope="class")
    def custom_instance(self):
        return Checkpoint("checkpoint_custom.csv", index=False, sep="\t")

    def test_transform(self, default_instance, custom_instance, dataframe):
        """Tests checkpoint file and return data fidelity to input data."""

        # tests default instance
        result = default_instance(dataframe)
        assert result.equals(dataframe)
        # must use float precision due to read_csv bad rounding precision
        result = read_csv(
            "checkpoint_default.csv", float_precision="round_trip"
        )
        assert result.equals(dataframe)

        # tests custom instance
        result = custom_instance(dataframe)
        assert result.equals(dataframe)
        # must use float precision due to read_csv bad rounding precision
        result = read_csv(
            "checkpoint_custom.csv", float_precision="round_trip", sep="\t"
        )
        assert result.equals(dataframe)

    @classmethod
    def teardown_class(cls):
        """Cleans up checkpoint files."""
        files = glob("./checkpoint*.csv")
        for f in files:
            os.remove(f)
