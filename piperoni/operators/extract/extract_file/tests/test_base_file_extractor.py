import pytest

from piperoni.operators.extract.extract_file.base import FileExtractor

"""
This module implements tests for the base FileExtractor.
"""


class TestFileExtractor:
    """Tests funtionality of FileExtractor"""

    @pytest.fixture(scope="class")
    def instance(self):
        return FileExtractor()

    @pytest.fixture(scope="class")
    def path(self):
        return "foo/bar/name.ext"

    def test_abstract_transform(self, instance):
        """Tests that transform raises a not-implemented error."""

        # tests when called directly
        with pytest.raises(NotImplementedError):
            instance.transform("a")

        # tests when called with object call literal
        with pytest.raises(NotImplementedError):
            instance("a")

    def test_inspect_extension(self, instance, path):
        """Tests the extension inspection static method"""

        assert instance.inspect_extension(path) == "ext"
