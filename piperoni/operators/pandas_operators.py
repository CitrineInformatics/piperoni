from piperoni.operators.base import BaseOperator
from pandas import DataFrame, concat
import inspect


class PandasConcatFiles(BaseOperator):
    def __init__(self, list_of_datafarmes, **kwargs):

        # validates that kwargs can be bound to pandas.concat
        signature = inspect.signature(concat)
        try:
            signature.bind([DataFrame(), DataFrame()], **kwargs)
        except TypeError:
            raise TypeError(
                "arguments must match the signature of pandas.concat"
            )
        else:
            self.list_of_dataframes_to_concat = list_of_datafarmes
            self.kwargs = kwargs

    def transform(self, df: DataFrame) -> DataFrame:
        """Performs a pandas concat.

        Parameters
        ----------
        df: DataFrame
            The data to pandas concat with.

        Returns
        -------
        DataFrame
            The modified data.
        """
        df = concat(self.list_of_dataframes_to_concat + [df], **self.kwargs)
        return df


class PandasMergeFiles(BaseOperator):
    def __init__(self, right_df, **kwargs):

        # validates that kwargs can be bound to pandas.DataFrame.merge
        signature = inspect.signature(DataFrame().merge)
        try:
            signature.bind(DataFrame(), **kwargs)
        except TypeError:
            raise TypeError(
                "arguments must match the signature of pandas.DataFrame.merge"
            )
        else:
            self.right_dataframe = right_df
            self.kwargs = kwargs

    def transform(self, df: DataFrame) -> DataFrame:
        """Performs a pandas merge.

        Parameters
        ----------
        df: DataFrame
            The data to pandas merge with.

        Returns
        -------
        DataFrame
            The modified data.
        """
        df = df.merge(self.right_dataframe, **self.kwargs)
        return df
