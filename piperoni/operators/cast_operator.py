from piperoni.operators.base import BaseOperator

from abc import abstractmethod

"""
This module implements the CastOperator object.

Operators are applied to inputs using the __call__ python API, which wraps
the `transform` methods implemented for each Operator.

Examples
--------
Functionality of the CastOperator::
  opp = CastOperator()
  input = "some data"
  output = opp(input)
"""


class CastOperator(BaseOperator):
    """
    A generic operator to apply to data. Functions like
    BaseOperator, but with the benefit of explicitly
    specifying the output type.
    """

    @property
    @abstractmethod
    def input_type(self) -> type:
        """Get the expected type of the input"""

    @property
    @abstractmethod
    def output_type(self) -> type:
        """Get the expected type of the output"""

    def check_input(self, input_) -> bool:
        """Checks whether output matches the expected output type."""
        return isinstance(input_, self.input_type)

    def check_output(self, output_) -> bool:
        """Checks whether output matches the expected output type."""
        return isinstance(output_, self.output_type)

    def __call__(self, *args, **kwargs) -> object:
        """Class instances emulate callable methods."""
        # TODO: Is this an issue?
        # TODO: Maybe we should switch from *args **kwargs to explicit single input
        if not self.check_input(args[0]):
            class_name = self.__class__.__name__
            raise TypeError(
                f"{class_name} requires that your input is of type {self.input_type}, but got {type(args[0])}"
            )
        output_ = self.transform(*args, **kwargs)
        if not self.check_output(output_):
            class_name = self.__class__.__name__
            raise TypeError(
                f"{class_name} requires that your output is of type {self.output_type}, but got {type(output_)}"
            )
        return output_
