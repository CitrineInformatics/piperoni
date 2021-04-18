import pytest

from copy import deepcopy

from piperoni.operators.base import BaseOperator
from piperoni.operators.pipe import Pipe

"""
Implements tests for the BaseOperator object.
"""


# implements two trivial transformers for the tests
class StrToInt(BaseOperator):
    def transform(self, input_: str) -> int:
        return int(input_)


class IntToStr(BaseOperator):
    def transform(self, input_: int) -> str:
        return str(input_)


class DidNotImplementTransform(BaseOperator):
    pass


class TestBaseOperator:
    """Tests the BaseOperator class."""

    def test_abstract_method(self):
        """Tests that `transform` must be implemented."""

        # does not implement transform
        with pytest.raises(TypeError):
            DidNotImplementTransform()


class TestPipe:
    """Tests the functionality of a pipe."""

    def test_pipe(self):
        """Tests that you can chain operators together in a pipe."""

        step1 = StrToInt()
        step2 = IntToStr()
        pipe = Pipe([step1, step2])

        input_ = "1"
        assert pipe(input_) == input_

    def test_pipe_only_accepts_operators(self):
        """Tests that a pipe can only be initialized from BaseOperator"""

        def str_to_int(string):
            return int(string)

        with pytest.raises(RuntimeError):
            Pipe([str_to_int])

    def test_pipe_of_pipes(self):
        """Tests that you can make a pipe of pipes."""

        pipe1 = Pipe([StrToInt(), IntToStr()])
        pipe2 = Pipe([StrToInt(), IntToStr()])
        super_pipe = Pipe([pipe1, pipe2])

        input_ = "1"
        assert super_pipe(input_) == input_
