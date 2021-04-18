.. _extractors:

==========
Extractors
==========

Extractors are operators that read data from files. The are essentially Casters because the input
is always a string, and the output is usually another datatype. Below are examples of using
extractors in :ref:`Pipe <pipes>` objects.

CSVExtractor
============

The CSVExtractor is used to read a csv file and return the data in a pandas DataFrame.

.. code-block:: python

    from piperoni.operators.extract.extract_file.csv_ import CSVExtractor
    from piperoni.operators.pipe import Pipe

    extractor_pipe = Pipe(
        [
            CSVExtractor()
        ]
    )

    extracted_data = extractor_pipe("path/to/file.csv")

ExcelExtractor
==============

The ExcelExtractor is used to read an excel file and return the data in a pandas DataFrame.

.. code-block:: python

    from piperoni.operators.extract.extract_file.excel import ExcelExtractor
    from piperoni.operators.pipe import Pipe

    extractor_pipe = Pipe(
        [
            ExcelExtractor()
        ]
    )

    extracted_data = extractor_pipe("path/to/file.xlsx")

JSONExtractor
=============

The JSONExtractor is used to read a json file and return the data in a pandas DataFrame.

.. code-block:: python

    from piperoni.operators.extract.extract_file.json_ import JSONExtractor
    from piperoni.operators.pipe import Pipe

    extractor_pipe = Pipe(
        [
            JSONExtractor()
        ]
    )

    extracted_data = extractor_pipe("path/to/file.json")

Custom Extractors
=================

You may very often need to make your own custom extractor which will be a superclass of any of the
above classes or the base FileExtractor. You need to define your own ``transform`` method that reads
the data, transforms it, and returns it in some fashion.

Below is an example of a custom extractor using the base FileExtractor. It reads a csv separated by
tabs, downscopes to a few specific columns, and returns a pandas DataFrame:

.. code-block:: python

    from piperoni.operators.extract.extract_file.base import FileExtractor
    from piperoni.operators.pipe import Pipe
    from pandas import DataFrame

    import pandas as pd

    class MyCustomExtractor(FileExtractor):
        def transform(self, path: str) -> DataFrame:
            """Parses a cif file. This is a demo, and does not really work. """
            raw_file = pd.read_csv(path, sepstr = "\t")
            data_dict = {}
            data_dict["uid"] = raw_file.iloc[2,1]
            data_dict["journal"] = raw_file.iloc[1,8]
            data_dict["sites"] = raw_file.iloc[1,10:21]
            output_df = DataFrame(data = data_dict)
            return output_df

    extractor_pipe = Pipe(
        [
            MyCustomExtractor()
        ]
    )

    extracted_data = extractor_pipe("path/to/file.csv")
