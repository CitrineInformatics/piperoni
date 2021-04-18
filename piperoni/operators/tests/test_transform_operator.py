import pytest

from copy import deepcopy

from piperoni.operators.transform_operator import TransformOperator
from piperoni.operators.pipe import Pipe

"""
Implements tests for the TransformOperator object.
"""


# implements two trivial transformers for the tests
class IntToStr(TransformOperator):
    """Invalid transformer. Changes the types."""

    def transform(self, input_: str) -> int:
        return str(input_)


class IntToInt(TransformOperator):
    """Valid transformer. Types are consistent."""

    def transform(self, input_: int) -> str:
        return int(input_)


class DidNotImplementTransform(TransformOperator):
    pass


class TestTransformOperator:
    """Tests the TransformOperator class."""

    def test_abstract_method(self):
        """Tests that `transform` must be implemented."""

        # does not implement transform
        with pytest.raises(TypeError):
            DidNotImplementTransform()
