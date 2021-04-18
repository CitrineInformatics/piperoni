import os
import pytest

import piperoni as etl

from pandas import read_csv

from piperoni.operators.extract.extract_file.csv_ import CSVExtractor

"""
This module implements tests for the CSVExtractor.
"""

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_TEST_FILE = os.path.join(
    ROOT_DIR,
    "..",
    "..",
    "..",
    "..",
    "..",
    "test_files",
    "Strehlow and Cook.csv",
)
TSV_TEST_FILE = os.path.join(
    ROOT_DIR,
    "..",
    "..",
    "..",
    "..",
    "..",
    "test_files",
    "Strehlow and Cook.tsv",
)

EXPECTED_CSV = read_csv(CSV_TEST_FILE)
EXPECTED_TSV = read_csv(TSV_TEST_FILE, sep="\t")


class TestCSVExtractor:
    """Tests funtionality of CSVExtractor"""

    @pytest.fixture(scope="class")
    def default_instance(self):
        return CSVExtractor()

    @pytest.fixture(scope="class")
    def custom_instance(self):
        return CSVExtractor(sep="\t")

    def test_instantiation(self):
        """Tests instantiation of class by kwargs."""

        # tests raising error when arguments do not match pandas.read_csv
        with pytest.raises(TypeError):
            CSVExtractor(invalid_argument="invalid")

    def test_transform(self, default_instance, custom_instance):
        """Tests that this transformer returns the same results at read_csv."""

        # tests default instance on csv file
        assert default_instance(CSV_TEST_FILE).equals(EXPECTED_CSV)

        # tests customized instance on a tsv file
        assert custom_instance(TSV_TEST_FILE).equals(EXPECTED_TSV)

        # using the default instance on the tsv should fail
        with pytest.raises(AssertionError):
            assert default_instance(TSV_TEST_FILE).equals(EXPECTED_TSV)
