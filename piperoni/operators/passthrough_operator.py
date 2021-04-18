from piperoni.operators.base import BaseOperator

from abc import abstractmethod
from copy import deepcopy
import pandas as pd

"""
This module implements the PassthroughOperator object.

Operators are applied to inputs using the __call__ python API, which wraps
the `transform` methods implemented for each Operator.

Examples
--------
Functionality of the PassthroughOperator::
  opp = PassthroughOperator()
  input = "some data"
  output = opp(input)
"""


class PassthroughOperator(BaseOperator):
    """
    Base operator for operations that should not
    modify the data and pass it through unchanged.
    """

    def test_equals(self, input_, output_) -> bool:
        if isinstance(input_, pd.DataFrame):
            return input_.equals(output_)
        else:
            return input_ == output_

    def error_message(self, input_, output_):
        return f"Output from passthrough operator changed from input {input_} -> {output_}"

    def __call__(self, *args, **kwargs) -> object:
        """Class instances emulate callable methods."""
        # TODO: Is this a problem?
        input_ = deepcopy(args[0])
        output_ = self.transform(*args, **kwargs)
        if not self.test_equals(input_, output_):
            raise RuntimeError(self.error_message(input_, output_))
        return output_
