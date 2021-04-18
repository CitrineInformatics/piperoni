import os
import pytest
import yaml

from pandas import DataFrame

import piperoni as hep

from piperoni.operators.transform.transform_name.header_map import HeaderMap


"""
This module implements tests for the HeaderMap.
"""


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG = os.path.join(
    ROOT_DIR, "..", "..", "..", "..", "..", "test_files", "header_map.yaml"
)
BAD_CONFIG = os.path.join(
    ROOT_DIR, "..", "..", "..", "..", "..", "test_files", "bad_header_map.yaml"
)

with open(CONFIG) as fh:
    MAP = yaml.load(fh, Loader=yaml.SafeLoader)
OLD_HEADERS = sorted(list(MAP.keys()))
NEW_HEADERS = [MAP[i] for i in OLD_HEADERS]


class TestHeaderMap:
    """Tests funtionality of HeaderMap."""

    # fixtures test that the class can be instantiated from yaml file
    @pytest.fixture(scope="class")
    def strict_instance(self):
        return HeaderMap.from_yaml(CONFIG, complete_map=True)

    @pytest.fixture(scope="class")
    def relaxed_instance(self):
        return HeaderMap.from_yaml(CONFIG, complete_map=False)

    @pytest.fixture(scope="class")
    def default_headers(self):
        return DataFrame(columns=OLD_HEADERS)

    @pytest.fixture(scope="class")
    def extra_headers(self):
        return DataFrame(columns=OLD_HEADERS + ["unknown_header"])

    def test_raises_exception_with_bad_yaml(self):
        """Tests that an exception is raised if a bad yaml is supplied.

        This config is a list and not a dictionary.
        """
        with pytest.raises(AssertionError):
            HeaderMap.from_yaml(BAD_CONFIG)

    def test_transform(
        self, strict_instance, relaxed_instance, default_headers, extra_headers
    ):
        """Tests a grid of cases:

        [strict, relaxed] x [extra headers, no extra headers]

        strict/relaxed refers to whether the transformer expects a complete map
        between old and new column headers, or whether there may be ommissions.
        """

        # strict case with extra headers (raises error for incomplete map)
        with pytest.raises(KeyError):
            strict_instance(extra_headers)

        # strict case with no surprise headers
        output = strict_instance(default_headers)
        for expected, result in zip(NEW_HEADERS, output.columns):
            assert expected == result

        # relaxed case with extra headers (unknow headers should remain same)
        output = relaxed_instance(extra_headers)
        for expected, result in zip(
            NEW_HEADERS + ["unknown_header"], output.columns
        ):
            assert expected == result

        # relaxed case with no surprise headers
        output = relaxed_instance(default_headers)
        for expected, result in zip(NEW_HEADERS, output.columns):
            assert expected == result
