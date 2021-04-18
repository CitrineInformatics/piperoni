import os
import pytest

import piperoni as etl

from piperoni.operators.extract.extract_file.json_ import JSONExtractor

"""
This module implements tests for the JSONExtractor.
"""

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_TEST_FILE = os.path.join(
    ROOT_DIR, "..", "..", "..", "..", "..", "test_files", "test.json"
)
SC_TEST_FILE = os.path.join(
    ROOT_DIR,
    "..",
    "..",
    "..",
    "..",
    "..",
    "test_files",
    "Strehlow and Cook.json",
)


class TestJSONExtractor:
    """Tests funtionality of JSONExtractor"""

    @pytest.fixture(scope="class")
    def with_flatten_and_tensors(self):
        return JSONExtractor(flatten=True, express_tensors=True)

    @pytest.fixture(scope="class")
    def with_flatten_no_tensors(self):
        return JSONExtractor(flatten=True, express_tensors=False)

    @pytest.fixture(scope="class")
    def with_raw(self):
        return JSONExtractor(flatten=False)

    def test_transform(
        self, with_flatten_and_tensors, with_flatten_no_tensors, with_raw
    ):
        """Tests loading data with the different optional arguments."""

        # tests that the lattice matrix is collected as nested lists
        data = with_flatten_and_tensors(JSON_TEST_FILE)
        int_list = data["foo|bar"][0]
        assert isinstance(int_list, list)
        for i, val in enumerate(int_list):  # tests ordering
            assert val == i + 1

        # tests that there are no list-like elements
        data = with_flatten_no_tensors(SC_TEST_FILE)
        for item in data.values.flatten():  # return type was dataframe
            assert not isinstance(item, list)

        # tests that the data is not unpacked at all
        data = with_raw(JSON_TEST_FILE)
        structure_field = data["foo"][0]  # structure should be packed
        assert isinstance(structure_field, dict)

        # tests with Strehlow and Cook
        data = with_raw(SC_TEST_FILE)
        cell_value = data["Band gap"][536]
        assert cell_value == 2.26
