import os
import pytest
import numpy as np
import pandas as pd

from piperoni.operators.pandas_operators import PandasMergeFiles
from piperoni.operators.pandas_operators import PandasConcatFiles


class TestPandasOperators:
    """Test the Pandas Operators"""

    @pytest.fixture(scope="class")
    def test_dataframe_1(self) -> pd.DataFrame:
        data = {
            "uid": ["a", "b", "c", "d"],
            "alpha": [232.0, 67.8, 2.5, 189.8],
            "bravo": ["hello", "world", "beep", "boop"],
        }
        return pd.DataFrame(data)

    @pytest.fixture(scope="class")
    def test_dataframe_2(self) -> pd.DataFrame:
        data = {
            "id": ["a", "b", "c", "d"],
            "charlie": [True, False, True, True],
            "delta": [1, 3, 5, 7],
        }
        return pd.DataFrame(data)

    @pytest.fixture(scope="class")
    def test_dataframe_3(self) -> pd.DataFrame:
        data = {"uid": ["e", "f"], "charlie": [True, False], "delta": [1, 3]}
        return pd.DataFrame(data)

    @pytest.fixture(scope="class")
    def ref_test_merge_dataframe(self) -> pd.DataFrame:
        data = {
            "uid": ["a", "b", "c", "d"],
            "alpha": [232.0, 67.8, 2.5, 189.8],
            "bravo": ["hello", "world", "beep", "boop"],
            "id": ["a", "b", "c", "d"],
            "charlie": [True, False, True, True],
            "delta": [1, 3, 5, 7],
        }
        return pd.DataFrame(data)

    @pytest.fixture(scope="class")
    def ref_test_concat_dataframe(self) -> pd.DataFrame:
        data = {
            "uid": ["a", "b", "c", "d", "e", "f"],
            "alpha": [232.0, 67.8, 2.5, 189.8, np.NaN, np.NaN],
            "bravo": ["hello", "world", "beep", "boop", np.NaN, np.NaN],
            "charlie": [np.NaN, np.NaN, np.NaN, np.NaN, True, False],
            "delta": [np.NaN, np.NaN, np.NaN, np.NaN, 1, 3],
        }
        return pd.DataFrame(data)

    def test_pandas_merge_file_operator(
        self,
        test_dataframe_1,
        test_dataframe_2,
        test_dataframe_3,
        ref_test_merge_dataframe,
        ref_test_concat_dataframe,
    ):
        """Test that keyword arguments passed and validated to pandas.DataFrame.merge"""

        # test correct merge output
        result = PandasMergeFiles(
            test_dataframe_2, how="left", left_on="uid", right_on="id"
        )(test_dataframe_1)
        pd.testing.assert_frame_equal(ref_test_merge_dataframe, result)

        # test incorrect kwards
        with pytest.raises(TypeError):
            PandasMergeFiles(
                test_dataframe_2,
                hello_world="left",
                left_on="uid",
                right_on="id",
            )(test_dataframe_1)

        # test correct concat output
        result = PandasConcatFiles([test_dataframe_3], ignore_index=True)(
            test_dataframe_1
        )
        result = result.sort_values(
            by=["uid"], ignore_index=True
        )  # sort on uid values before comparison
        result = result[
            ref_test_concat_dataframe.columns.tolist()
        ]  # sort on columns before comparison

        pd.testing.assert_frame_equal(result, ref_test_concat_dataframe)

        # test incorrect kwards
        with pytest.raises(TypeError):
            PandasConcatFiles(
                [test_dataframe_3], beep_boop_bap=1000, ignore_index=True
            )(test_dataframe_1)
