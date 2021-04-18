from functools import reduce

from typing import List

from abc import ABC, abstractmethod

import logging
import pandas as pd
import warnings

"""
This module implements the BaseOperator object.

Operators are applied to inputs using the __call__ python API, which wraps
the `transform` methods implemented for each Operator.

Examples
--------
Functionality of the BaseOperator::
  opp = BaseOperator()
  input = "some data"
  output = opp(input)
"""


class BaseOperator(ABC):
    """A generic operator to apply to data."""

    @property
    def logger(self):
        return logging.getLogger("base")

    @abstractmethod
    def transform(self, input_: object) -> object:
        """Implemented in concrete sub-classes of BaseOperator.

        Concrete implementations of this method should accept a single argument
        as input and return a single output (most likely pandas DataFrame).

        Parameters
        ----------
        input_: object
            The single argument for transform.

        Returns
        -------
        object
            The single output of transform.
        """
        pass

    def __call__(self, *args, **kwargs) -> object:
        """Class instances emulate callable methods."""
        return self.transform(*args, **kwargs)
