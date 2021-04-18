import pytest
import warnings
import pandas as pd
import os
import piperoni

from piperoni.operators.analyze.comparer import CompareOperator

"""
This module implements tests for the Comparer.
"""


class TestComparer:
    """Tests funtionality of Comparer"""

    def import_and_uid_df(self):

        ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(
            ROOT_DIR, "..", "..", "..", "..", "test_files", "test.csv"
        )
        df = pd.read_csv(file_path)
        df["uid"] = [a for a in range(len(df.index))]
        return df

    @pytest.fixture(scope="class")
    def dataframe(self):
        return self.import_and_uid_df()

    @pytest.fixture(scope="class")
    def modified_dataframe(self):
        df = self.import_and_uid_df()
        df.iloc[1, 1] = 9
        return df

    def test_simple_run(self, dataframe):
        comparer = CompareOperator(dataframe, "uid")
        return comparer(dataframe)

    def test_transform(self, dataframe, modified_dataframe):
        """Tests comparer."""

        # tests identical dfs
        comparer = CompareOperator(dataframe, "uid")
        with pytest.warns(UserWarning) as record:
            comparer.transform(dataframe)
        assert (
            record[0].message.args[0]
            == "All rows in input are consistent with the reference in overlapping columns."
        )

        # tests different dfs
        with pytest.warns(UserWarning) as record:
            comparer.transform(modified_dataframe)
        assert (
            record[0].message.args[0]
            == "Row 1 has values [9] in input data, but has values [6] in the reference for columns ['prop1']"
        )
