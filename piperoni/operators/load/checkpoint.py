"""Implements checkpoints, which save intermediate data to a file."""

from pandas import DataFrame
from piperoni.operators.passthrough_operator import PassthroughOperator


class Checkpoint(PassthroughOperator):
    """A checkpoint for saving the state of data in a pipe.

    The checkpoint is saved in CSV format.

    Parameters
    ----------
    path: str
        File path for the checkpoint file.
    kwargs: keyword arguments
        Customizes the checkpoint file. Keyword arguments passed to the
        DataFrame.to_csv method
    """

    def __init__(self, path: str, **kwargs: dict):
        self.path = path
        self.kwargs = kwargs

    def test_equals(self, input_, output_) -> bool:
        return input_.equals(output_)

    def transform(self, df: DataFrame) -> DataFrame:
        """Saves data and continues the pipe.

        Parameters
        ----------
        df: DataFrame
            The input data to checkpoint.

        Returns
        -------
        DataFrame
            The unaltered data.
        """
        df.to_csv(self.path, **self.kwargs)
        return df
