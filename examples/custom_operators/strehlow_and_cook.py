from piperoni.operators.base import BaseOperator

from copy import deepcopy

from pandas import DataFrame


"""Custom operators for the Strehlow and Cook dataset.
"""


class FixBandgapUnits(BaseOperator):
    """Deals with Bandgap outliers in S&C.

    Reduces outliers by a factor of 1000. A materials
    scientist used meV insead of eV for some of the entries.

    Parameters
    ----------
    column: string
        The column to fix outliers.
    threshold: float
        The threshold, above which outliers are fixed.
    """

    def __init__(self, column, threshold=50):
        self.column = column
        self.threshold = threshold

    def transform(self, df: DataFrame) -> DataFrame:
        """Reduces outliers by a factor of 1000.

        Parameters
        ----------
        df: DataFrame
            Input dataframe that may contain outliers.

        Returns
        -------
        DataFrame
            Has the outliers reduced by a factor of 1000.
        """

        df = deepcopy(df)  # don't overwrite initial df
        original_values = df[self.column]

        # collects transformed values
        transformed_values = []
        for val in original_values:
            if val >= self.threshold:
                val = val / 1000
            transformed_values.append(val)

        # returns edited version of df
        df[self.column] = transformed_values
        return df
