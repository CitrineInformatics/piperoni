from pandas import read_excel, DataFrame

from piperoni.operators.extract.extract_file.base import FileExtractor

"""
This module implements objects for extracting data from excel files.
"""


class ExcelExtractor(FileExtractor):
    """Extracts data from excel files.

    The default behavior is to load all of the sheets. The data in each sheet
    must be aligned in the top left corner and all of the column headers must
    be named.

    Parameters
    ----------
    sheet_name: str|int|list|None
        Determines which sheets to return. See pandas.read_excel for
        documentation.
    kwargs: dict
        Keyword values used to customize extraction. See pandas.read_excel for
        supported arguments.

    Caution
    -------
    An error will not be raised during instantiation if kwargs do not
    match the arguments in pandas.read_excel, so be careful.
    """

    def __init__(self, sheet_name=None, **kwargs):
        self.kwargs = kwargs
        self.kwargs.update({"sheet_name": sheet_name})

    @property
    def output_type(self):
        return dict

    @property
    def output_subtypes(self):
        return {"k": str, "v": DataFrame}

    def check_output(self, output_):
        if type(output_) == self.output_type:
            keys = []
            vals = []
            for k, v in output_.items():
                keys.append(type(k) == self.output_subtypes["k"])
                vals.append(type(v) == self.output_subtypes["v"])
            if all(keys) and all(vals):
                return True
        return False

    def transform(self, path: str) -> dict:
        """Returns DataFrames from the excel file.

        Parameters
        ----------
        path : str
            Path to excel file.

        Returns
        -------
        dict
            A dictionary of pandas DataFrames. Keys are sheet names.

        Raises
        ------
        Exception
            If there are unnamed column headers or the table is not aligned in
            the top left corner of each sheet.
        """
        data = read_excel(path, engine="openpyxl", **self.kwargs)
        if isinstance(data, DataFrame):  # read_excel can return a single df
            data = {"sheet": data}

        # column headers and location of table in sheet
        for sheet, frame in data.items():
            for header in frame.columns:
                try:
                    assert not header.startswith("Unnamed")
                except AssertionError:
                    raise Exception(
                        "Make sure the table in {} is aligned in the top-left "
                        "corner and all columns have headers".format(sheet)
                    )

        return data
