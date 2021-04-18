from piperoni.operators.transform_operator import TransformOperator

from pandas import DataFrame
from uuid import uuid1

"""
This module implements an operator that can assign UIDs and update them as new rows are added.
"""


class AssignUIDsOperator(TransformOperator):
    """
    Assigns and updates UIDs as necessary.
    By default, prepends with a unique uuid signature.

    Parameters
    ----------
    uid_column : str
        Column name to add UIDs to
    uid_start : int, optional
        Start of UID numerical increment, by default 0
    unique_prepend : str, optional
        A string to prepend numerical increments with, by default str(uuid1())+' - '
    """

    def __init__(
        self, uid_column, uid_start=0, unique_prepend=str(uuid1()) + " - "
    ):
        self.uid_column = uid_column
        self.uid_counter = uid_start
        self.unique_prepend = unique_prepend

    def transform(self, df: DataFrame):
        """
        Adds new UIDs to or completes missing UIDs in a dataframe.

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe to augment with UIDs.

        Returns
        -------
        pd.DataFrame
            Dataframe augmented with UIDs.
        """

        if self.uid_column not in df.columns:
            df[self.uid_column] = [
                f"{self.unique_prepend}{i}" for i in range(len(df.index))
            ]
            self.uid_counter = len(df.index)

        else:
            ids = df[self.uid_column].values
            if any(np.isnan(ids)):
                max_uid = self.uid_counter
                for i, uid in enumerate(ids):
                    if np.isnan(uid):
                        self.uid_counter += 1
                        ids[i] = f"{self.unique_prepend}{self.uid_counter}"
            df[self.uid_column] = ids

        return df
