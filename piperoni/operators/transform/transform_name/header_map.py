"""This module implements objects for transforming the name-space of data."""
import yaml

from copy import deepcopy

from pandas import DataFrame

from piperoni.operators.base import BaseOperator


class HeaderMap(BaseOperator):
    """
    Replaces the headers of a pandas DataFrame according to a key-value map.

    The key-value map can be supplied as a python dictionary or loaded from
    a yaml file. The keys are the current column headers, and the values are
    the new column headers.

    Parameters
    ----------
    config: dict
        Key-value mapping between current headers and new headers. Example
        data structure: {'old_header': 'new_header'}.
    complete_map: bool
        Whether a key-value map is required for all column headers. Defaults
        to True, in which case, all column headers passed to transform must
        have a key in the config. If False, then current headers that are not
        in the map will cary over to the new headers.
    """

    def __init__(self, config: dict, complete_map: bool = True):
        self.config = config
        self.complete_map = complete_map

    @classmethod
    def from_yaml(cls, path: str, **kwargs):
        """
        Constructs a HeaderMap from a yaml file.

        Parameters
        ----------
        path: str
            File path to the encoded mapping.
        kwargs: keyword arguments
            Keyword arguments passed to the constructor.

        Returns
        -------
        HeaderMap
            An instance of a mapping operator.

        Raises
        ------
        AssertionError
            If loading the yaml file does not return a dictionary object.
        """
        with open(path) as fh:
            config = yaml.load(fh, Loader=yaml.SafeLoader)
        assert isinstance(config, dict)
        return cls(config, **kwargs)

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Converts column headers using the config key-value pairs.

        Parameters
        ----------
        df: DataFrame
            The pandas DataFrame to transform

        Returns
        -------
        DataFrame
            Copy of the original df with replaced headers.

        Raises
        ------
        KeyError
            If complete_map is set to true, then it is required that all of
            the old column headers are represented in the config.
        """
        df = deepcopy(df)  # does not overwrite original df
        old_headers = df.columns

        if self.complete_map:  # will raise error if old header not present
            new_headers = [self.config[key] for key in old_headers]
        else:  # defaults to the old header if not present in config
            new_headers = [self.config.get(key, key) for key in old_headers]

        df.columns = new_headers
        return df
