import os
import pytest

from pandas import DataFrame

import piperoni as etl

from piperoni.operators.extract.extract_file.excel import ExcelExtractor

"""
This module implements tests for the ExcelExtractor.
"""

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
EXCEL_TEST_FILE = os.path.join(
    ROOT_DIR,
    "..",
    "..",
    "..",
    "..",
    "..",
    "test_files",
    "Strehlow and Cook.xlsx",
)


class TestExcelExtractor:
    """Tests funtionality of ExcelExtractor"""

    @pytest.fixture(scope="class")
    def all_sheets(self):
        return ExcelExtractor()

    @pytest.fixture(scope="class")
    def only_one_sheet(self):
        return ExcelExtractor(sheet_name="Strehlow and Cook")

    def test_transform(self, all_sheets, only_one_sheet):
        """Tests loading data with the different optional arguments."""

        # tests gathering all sheets
        # one of the sheets does not have an aligned table
        with pytest.raises(Exception):
            assert all_sheets(EXCEL_TEST_FILE)

        # tests gathering only one sheet
        data = only_one_sheet(EXCEL_TEST_FILE)
        assert isinstance(data, dict)
        sheet = data["sheet"]
        assert isinstance(sheet, DataFrame)
        cell_value = sheet["Band gap"][536]
        assert cell_value == 2.26
