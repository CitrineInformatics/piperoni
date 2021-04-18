from piperoni.operators.base import BaseOperator

"""
This module implements the TransformOperator object.

Operators are applied to inputs using the __call__ python API, which wraps
the `transform` methods implemented for each Operator.

Examples
--------
Functionality of the TransformOperator::
  opp = TransformOperator()
  input = "some data"
  output = opp(input)
"""


class TransformOperator(BaseOperator):
    """
    A generic operator to apply to data. Functions like
    BaseOperator, but with the benefit of validating that
    input and output types are the same.
    """

    def __call__(self, *args, **kwargs) -> object:
        """Class instances emulate callable methods."""
        input_ = args[0]
        output_ = self.transform(*args, **kwargs)
        if not type(input_) == type(output_):
            class_name = self.__class__.__name__
            raise TypeError(
                f"{class_name} requires that your input and output types are the same type."
            )
        return output_
