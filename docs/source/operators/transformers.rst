.. _transformers:

============
Transformers
============

Transformers are operators that change data while keeping input/output datatypes the same.

HeaderMap
=========

The HeaderMap transformer is used to change the column headers of an input pandas DataFrame.

Normalizer
==========

The Normalizer transformer is used to shift the mean of an input pandas DataFrame.

Custom Transformers
===================

You may often need to make your own custom transformer which will be a superclass of
TransformOperator. You need to define your own ``transform`` method that transforms the data and
returns it as the same datatype.

Below is an example of a custom extractor using the base FileExtractor. It reads a csv separated by
tabs, downscopes to a few specific columns, and returns a pandas DataFrame:

.. code-block:: python

    from piperoni.operators.transform_operator import TransformOperator
    from piperoni.operators.pipe import Pipe
    from pandas import DataFrame

    class IncrementNumericByOneTransformer(TransformOperator):
        def transform(self, input_: DataFrame) -> DataFrame:
            df = deepcopy(input_)
            numeric_cols = [col for col in df if df[col].dtype.kind != 'O']
            df[numeric_cols] += 1
            return df

    extractor_pipe = Pipe(
        [
            MyCustomExtractor(),
            IncrementNumericByOneTransformer(),
        ]
    )
