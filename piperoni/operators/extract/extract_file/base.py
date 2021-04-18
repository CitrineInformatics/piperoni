import os

from pandas import DataFrame

from piperoni.operators.cast_operator import CastOperator

"""
This module implements the base FileExtractor object.

File Extractors generally accept file paths as input and return DataFrames.
"""


class FileExtractor(CastOperator):
    """Extracts data from file storage."""

    def transform(self, path: str) -> DataFrame:
        """Implemented in classes inheriting from FileExtractor."""
        raise NotImplementedError(
            "transform is implemented in classes inheriting from FileExtractor"
        )

    @property
    def input_type(self):
        return str

    @property
    def output_type(self):
        """Implemented in classes inheriting from FileExtractor."""
        raise NotImplementedError(
            "output_type is implemented in classes inheriting from FileExtractor"
        )

    @staticmethod
    def inspect_extension(path: str) -> str:
        """Returns the extension of the file in path.

        Parameters
        ----------
        path : str
            Path to a file.

        Returns
        -------
        str
            The file extension.
        """

        path_name, ext = os.path.splitext(path)
        return ext[1:]  # don't return the period "." in the extension
