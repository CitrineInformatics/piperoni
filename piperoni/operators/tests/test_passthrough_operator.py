import pytest

from copy import deepcopy

from piperoni.operators.passthrough_operator import PassthroughOperator
from piperoni.operators.pipe import Pipe

"""
Implements tests for the PassthroughOperator object.
"""


class StrToStrValid(PassthroughOperator):
    """Test passthrough with str"""

    def transform(self, input_: str) -> str:
        return input_


class StrToStrInvalid(PassthroughOperator):
    """Test passthrough with str and catch modification"""

    def transform(self, input_: str) -> str:
        return input_ + "_modified"


class CustomTestEqualsWithListValid(PassthroughOperator):
    """Transform with custom equals function"""

    def test_equals(self, input_, output_) -> bool:
        if not len(input_) == len(output_):
            return False
        return all([a == b for a, b in zip(input_, output_)])

    def transform(self, input_: list) -> list:
        return input_


class CustomTestEqualsWithListInvalid1(PassthroughOperator):
    """Transforms the same type validly"""

    def test_equals(self, input_, output_) -> bool:
        if not len(input_) == len(output_):
            return False
        return all([a == b for a, b in zip(input_, output_)])

    def transform(self, input_: list) -> list:
        return input_ + ["a"]


class CustomTestEqualsWithListInvalid2(PassthroughOperator):
    """Transforms the same type validly"""

    def test_equals(self, input_, output_) -> bool:
        if not len(input_) == len(output_):
            return False
        return all([a == b for a, b in zip(input_, output_)])

    def transform(self, input_: list) -> list:
        input_.reverse()
        return input_


class CustomErrorMessage(PassthroughOperator):
    def error_message(self, input_, output_):
        return "Custom message"

    def transform(self, input_):
        return False


class TestPassthroughOperator:
    """Tests the PassthroughOperator class."""

    def test_customerrormessage(self):
        with pytest.raises(RuntimeError, match="Custom message") as record:
            CustomErrorMessage()("input")

    def test_strtostrvalid(self):
        assert StrToStrValid()("input") == "input"

    def test_strtostrinvalid(self):
        with pytest.raises(RuntimeError) as record:
            StrToStrInvalid()("input")

    def test_customtestequalswithlistvalid(self):
        assert CustomTestEqualsWithListValid()(["a", "b"]) == ["a", "b"]

    def test_customtestequalswithlistinvalid1(self):
        with pytest.raises(RuntimeError) as record:
            CustomTestEqualsWithListInvalid1()(["a", "b"])

    def test_customtestequalswithlistinvalid2(self):
        with pytest.raises(RuntimeError) as record:
            CustomTestEqualsWithListInvalid2()(["a", "b"])
