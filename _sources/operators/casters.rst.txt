.. _casters:

=======
Casters
=======

A caster is an operator used for specifying a change in data type between its input and output.
Casters explicitly define ``input_type`` and ``output_type`` property methods and a ``transform``
method. You define your own casters using the CastOperator base class.

Custom Casters
==============

Below is an example of a custom caster that expects a dict as an input and a pandas DataFrame as an
output. Its transformation creates a DaraFrame from the dictionary input.

.. code-block:: python

    from piperoni.operators.cast_operator import CastOperator
    from pandas import DataFrame

    class ToDataFrameCaster(CastOperator):
        @property
        def input_type(self) -> type:
            return dict

        @property
        def output_type(self) -> type:
            return DataFrame
    
        def transform(self, input_) -> DataFrame:
            return DataFrame(self.input_)
