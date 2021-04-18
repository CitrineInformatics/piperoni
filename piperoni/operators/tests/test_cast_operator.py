import pytest

from copy import deepcopy

from piperoni.operators.cast_operator import CastOperator
from piperoni.operators.pipe import Pipe

"""
Implements tests for the CastOperator object.
"""


# implements two trivial transformers for the tests
class StrToIntValid(CastOperator):
    """Changes the types validly"""

    @property
    def input_type(self) -> type:
        return str

    @property
    def output_type(self) -> type:
        return int

    def transform(self, input_: str) -> int:
        return int(input_)


class IntToStrValid(CastOperator):
    """Transforms the same type validly"""

    @property
    def input_type(self) -> type:
        return int

    @property
    def output_type(self) -> type:
        return str

    def transform(self, input_: int) -> str:
        return str(input_)


class IntToStrInvalidOut(CastOperator):
    """Changes the types invalidly"""

    @property
    def input_type(self) -> type:
        return int

    @property
    def output_type(self) -> type:
        return int

    def transform(self, input_: int) -> str:
        return str(input_)


class IntToStrInvalidIn(CastOperator):
    """Changes the types invalidly"""

    @property
    def input_type(self) -> type:
        return str

    @property
    def output_type(self) -> type:
        return str

    def transform(self, input_: int) -> str:
        return str(input_)


class DidNotImplementTransformAndTypes(CastOperator):
    pass


class DidNotImplementTransform(CastOperator):
    @property
    def input_type(self) -> str:
        return str

    @property
    def output_type(self) -> str:
        return str


class DidNotImplementOutputType(CastOperator):
    @property
    def input_type(self) -> str:
        return str

    def transform(self, input_: int) -> str:
        return str(input_)


class DidNotImplementInputType(CastOperator):
    @property
    def output_type(self) -> str:
        return str

    def transform(self, input_: int) -> str:
        return str(input_)


class TestCastOperator:
    """Tests the CastOperator class."""

    def test_both_abstract_methods(self):
        """Tests that `transform` and `output_type` must be implemented."""

        # does not implement transform and output_type
        with pytest.raises(TypeError):
            DidNotImplementTransformAndTypes()

    def test_transform_abstract_method(self):
        """Tests that `transform` must be implemented."""

        # does not implement transform
        with pytest.raises(TypeError):
            DidNotImplementTransform()

    def test_output_type_abstract_method(self):
        """Tests that `output_type` must be implemented."""

        # does not implement output_type
        with pytest.raises(TypeError):
            DidNotImplementOutputType()


class TestPipe:
    """Tests the functionality of a pipe with TransforOperators."""

    def test_pipe(self):
        """Tests that you can chain operators together in a pipe."""

        step1 = StrToIntValid()
        step2 = IntToStrValid()
        step3 = IntToStrInvalidOut()
        step4 = IntToStrInvalidIn()
        pipe1 = Pipe([step1, step2])
        pipe2 = Pipe([step1, step3])
        pipe3 = Pipe([step1, step4])

        input_ = "1"
        assert pipe1(input_) == input_

        with pytest.raises(
            TypeError, match=".* requires that your output is of type .*"
        ):
            pipe2(input_)

        with pytest.raises(
            TypeError, match=".* requires that your input is of type .*"
        ):
            pipe3(input_)
