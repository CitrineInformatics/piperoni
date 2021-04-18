import inspect

from pandas import DataFrame, read_csv

from piperoni.operators.extract.extract_file.base import FileExtractor

"""
This module implements objects for extracting data from csv-like files.
"""


class CSVExtractor(FileExtractor):
    """Extracts data from csv files.

    Parameters
    ----------
    kwargs : dict
        Keyword values used to customize extraction. See pandas.read_csv for supported arguments.

    Raises
    ------
    TypeError
        If the kwargs do not match the pandas.read_csv signature.
    """

    def __init__(self, **kwargs):
        # validates that kwargs can be bound to pandas.read_csv
        signature = inspect.signature(read_csv)
        try:
            signature.bind("dummy_path.csv", **kwargs)
        except TypeError:
            raise TypeError(
                "arguments must match the signature of pandas.read_csv"
            )
        else:
            self.kwargs = kwargs

    @property
    def output_type(self):
        return DataFrame

    def transform(self, path: str) -> DataFrame:
        """Returns a DataFrame representation of the csv data.

        Parameters
        ----------
        path : str
            Path to csv file.

        Returns
        -------
        DataFrame
            Data contained in the file.
        """
        return read_csv(path, **self.kwargs)
