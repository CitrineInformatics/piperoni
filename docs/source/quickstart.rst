.. _quickstart:

==========
Quickstart
==========

Creating Operators
==================

Below are a few Operators that can be run in a Pipe for an ETL project:

.. code-block:: python

    from piperoni.operators.transform_operator import TransformOperator

    class IntToStr(TransformOperator):
        """Invalid transformer. Changes the types."""

        def transform(self, input_: str) -> int:
            return str(input_)


    class IntToInt(TransformOperator):
        """Valid transformer. Types are consistent."""

        def transform(self, input_: int) -> str:
            return int(input_)

Creating and Running a Pipe
===========================

The Operators above are run through the pipe below:

.. code-block:: python

    from piperoni.operators.pipe import Pipe

    # initialize operators
    step1 = StrToInt()
    step2 = IntToStr()

    # initialize pipe
    pipe = Pipe([step1, step2])

    # run pipe with input; get final data
    final_data = Pipe("1")

End To End CSV Manipulation Example
===================================

Take an example CSV that looks like the following:

.. list-table::
   :widths: 10 10 10
   :header-rows: 1

   * - Color
     - RGBColor
     - RGBValue
   * - darkorchid
     - red
     - 153
   * - darkorchid
     - green
     - 50
   * - darkorchid
     - blue
     - 204

The CSV is saved in a file in the root directory called "color.csv".
The following is a walk-through example of how to extract the CSV, manipulate it so that each color is a row, and save the csv to a new file.


.. code-block:: python

    from piperoni.operators.extract.extract_file.csv_ import CSVExtractor
    from piperoni.operators.transform_operator import TransformOperator
    from piperoni.operators.pipe import Pipe
    import pandas as pd
    from os import path as osp


    # Add kwargs for pandas pivot_table
    class PivotDf(TransformOperator):
        def __init__(self, **kwargs):
            self.__dict__.update(**kwargs)
            self.kwargs = kwargs

        def transform(self, input_: str):
            return pd.pivot_table(input_, **self.kwargs).reset_index().rename_axis(None, axis=1)



    simplePipe = Pipe([
        # input is the filepath
        CSVExtractor(),

        # input_ is being passed in from the CSVExtractor() output, and the args defined here are for pivot_table
        PivotDf(values = 'RGBValue', index="Color", columns="RGBColor")
    ],
        # logging in root directory
        logging_path='')

    ## first input
    filepath = osp.join(".",'colors.csv')

    # to view transformed csv
    output = simplePipe(filepath)

Now that the table has been pivoted, the new CSV will look like the following: 

.. list-table::
   :widths: 10 10 10 10
   :header-rows: 1

   * - Color
     - blue
     - green
     - red
   * - darkorchid
     - 204
     - 50
     - 153
