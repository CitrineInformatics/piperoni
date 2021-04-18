import pytest

from piperoni.operators.transform_operator import TransformOperator
from piperoni.operators.cast_operator import CastOperator
from piperoni.operators.passthrough_operator import PassthroughOperator
from piperoni.operators.pipe import Pipe
from piperoni.operators.pipeline import Pipeline, PipelineData


class PrintOperator(PassthroughOperator):
    def transform(self, input_):
        print(input_)
        return input_


class IncrementOperator(TransformOperator):
    """
    Hey it's me again.. just letting you know that this operator adds
    one to whatever you pass in, so the input must be a float or int type.
    One output, 1 plus the input. Really cool!
    """

    def transform(self, input_):
        print(f"Received {input_}, passing {input_+1}.")
        return input_ + 1


class SumOperator(CastOperator):
    """
    Hello World.
    This is my documentation for my operator that adds values in
    an interable. It's pretty sophisticated if you ask me. I mean come on!?
    There's one output, it's the sum of the input. Pretty dope.
    """

    @property
    def input_type(self):
        return PipelineData

    @property
    def output_type(self):
        return int

    def transform(self, input_):
        to_sum = list(input_.values())
        print(f"Summing {to_sum}, returning {sum(to_sum)}.")
        return sum(to_sum)


class OneVsRestSplitOperator(CastOperator):
    """
    Yooooo! This operator has two outputs:
        1. The value one, 1
        2. One subtracting from the input
    """

    def __init__(self, name):
        self.name = name

    @property
    def input_type(self):
        return int

    @property
    def output_type(self):
        return PipelineData

    def transform(self, input_):
        print(f"Received {input_}, splitting into {1} and {input_-1}.")
        return_data = PipelineData()
        return_data[f"{self.name}_output1"] = 1
        return_data[f"{self.name}_output2"] = input_ - 1
        return return_data


class TestPipeline:
    pipe1 = Pipe(
        [IncrementOperator(), IncrementOperator(), IncrementOperator()],
        name="Pipe1",
    )

    pipe2 = Pipe(
        [
            SumOperator(),
            IncrementOperator(),
            OneVsRestSplitOperator("pipe2"),
        ],
        name="Pipe2",
    )

    pipe3 = Pipe(
        [SumOperator(), OneVsRestSplitOperator("pipe3")], name="Pipe3"
    )

    # ----------------------------------------

    inputs = {
        pipe1: "pipe1_raw",
        pipe2: ["pipe1_output", "pipe2_raw"],
        pipe3: ["pipe1_output", "pipe2_output1", "pipe3_raw"],
    }

    outputs = {
        pipe1: "pipe1_output",
        pipe2: ["pipe2_output1", "pipe2_output2"],
        pipe3: ["pipe3_output1", "pipe3_output2"],
    }

    raws = {"pipe1_raw": 3, "pipe2_raw": 7, "pipe3_raw": 13}

    pipeline = Pipeline(inputs, outputs, raws)
    output = pipeline.run()
    pipeline.visualize(full=False)

    assert output == {
        "pipe2_output2": 13,
        "pipe3_output2": 19,
        "pipe3_output1": 1,
    }
