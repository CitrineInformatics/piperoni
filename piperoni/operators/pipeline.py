from piperoni.operators.pipe import Pipe
from piperoni.operators.base import BaseOperator
from dagre_py.core import plot
import copy

"""
This module implements Pipeline object.

Pipeline is a completely optional addon that can wrap multiple pipes,
and provide smart branching.

Outputs that are being branched need to be dicts with the keys
matching the input/output descriptors.

"""


class PipelineData(dict):
    pass


class Pipeline:
    """
    Encapsulates several pipes with branching outputs to provide
    smart cacheing and piping.

    Piping is done via codenames. If a pipe output codename matches
    another pipes input codename, the output from the first will be
    inputted into the second. Branching is done by pipes outputtting
    dicts.


    Parameters
    ----------
    inputs_dict: dict
        A dict where keys are Pipes and values are strings
        or a list of strings. Strings are codenames for what will
        be fed into Pipes.

    outputs_dict: dict
        A dict where keys are Pipes and values are strings
        or a list of strings. Strings are codenames for what comes
        out of pipes.

    raw_inputs: dict
        A dict where keys are Pipes and values are raw inputs.
        This is for pipes that have inputs that are not outputs
        of other pipes.

    """

    # TODO: Need to figure out a way to detect all pipes with
    # multiple outputs to check against CastOperator with dict.

    # TODO: Make sure internal caching is safe! If an input is used
    # multiple times it should not be modified! This can be ensured
    # by a simple deepcopy of the input by the transform
    # Generally, we should come up with a consistent deepcopy scheme
    # for piperoni. Currently it's a bit random.
    def __init__(self, inputs_dict, outputs_dict, raw_inputs):

        # TODO: Input validation

        self.inputs_dict = self._listify_dict_values(inputs_dict)
        self.outputs_dict = self._listify_dict_values(outputs_dict)
        self.raw_inputs = self._listify_dict_values(raw_inputs)

        self.results_dict = raw_inputs

        # TODO: Add check that an output is not duplicated!
        self.all_inputs_list = self._gather_listed_values(self.inputs_dict)
        self.all_outputs_list = self._gather_listed_values(self.outputs_dict)

        # These are calculated accurately, but are currently not used
        # They should be used for validation upon creating a Pipeline object
        # Or for runtime if Raw Inputs is no longer provided at object creation time

        # A simple and useful validation would be to check:
        # raw_inputs_inferred == raw_inputs

        # Another useful thing to report back to the user would be
        # to list the final_outputs_inferred and see if it makes sense
        self.raw_inputs_inferred = list(
            set(self.all_inputs_list) - set(self.all_outputs_list)
        )
        self.final_outputs_inferred = list(
            set(self.all_outputs_list) - set(self.all_inputs_list)
        )

        # TODO: Add logger and cross-checking for inferred vs provided raw inputs.

    def _listify_dict_values(self, d):
        new_dict = PipelineData()
        for k, v in d.items():
            if not isinstance(v, list):
                new_dict[k] = [v]
            else:
                new_dict[k] = v
        return new_dict

    def _gather_listed_values(self, d):
        master_list = []
        for v in d.values():
            master_list += v
        # TODO: See Todo above
        return master_list

    def _gather_prerequisite_pipes_for_outputs(self, outputs):
        pipes_required = []
        for output in outputs:
            for k, v in self.outputs_dict.items():
                if output in v:
                    pipes_required.append(k)
        return list(set(pipes_required))

    def _gather_pipes_on_inputs(self, inputs):
        pipes_receiving = []
        for pipe_input in inputs:
            for k, v in self.inputs_dict.items():
                if pipe_input in v:
                    pipes_receiving.append(k)
        return list(set(pipes_receiving))

    # TODO: See above, make sure if a cahced input
    # is used multiple times, it is deepcopied somewhere!
    def _recursive_execute_pipe(self, pipe):
        pipe_inputs = self.inputs_dict[pipe]
        pipe_outputs = self.outputs_dict[pipe]

        # Check if a cached result exists
        if len(pipe_outputs) == 1:
            if pipe_outputs[0] in self.results_dict:
                return self.results_dict[pipe_outputs[0]]
        elif all(
            [pipe_output in self.results_dict for pipe_output in pipe_outputs]
        ):
            return_dict = PipelineData()
            for pipe_output in pipe_outputs:
                return_dict[pipe_output] = self.results_dict[pipe_output]
            return return_dict

        # If no cached result exists, must execute pipe and cache it
        # But for execution, we need to check if all its inputs are calculated.
        missing_inputs = [
            pipe_input
            for pipe_input in pipe_inputs
            if pipe_input not in self.results_dict
        ]
        if missing_inputs:
            # If not, we need to get those first
            for (
                prerequisite_pipe
            ) in self._gather_prerequisite_pipes_for_outputs(missing_inputs):
                self._recursive_execute_pipe(prerequisite_pipe)

        # There should no longer be any missing inputs
        if len(pipe_inputs) == 1:
            pipe_results = pipe(self.results_dict[pipe_inputs[0]])
        else:
            pipe_inputs_dict = PipelineData()
            for pipe_input in pipe_inputs:
                pipe_inputs_dict[pipe_input] = self.results_dict[pipe_input]
            pipe_results = pipe(pipe_inputs_dict)

        if isinstance(pipe_results, PipelineData):
            self.results_dict.update(pipe_results)
        else:
            self.results_dict[pipe_outputs[0]] = pipe_results

        return pipe_results

    # TODO: Wipe internal state every run?
    def run(self):
        return_dict = {}
        for prerequisite_pipe in self._gather_prerequisite_pipes_for_outputs(
            self.final_outputs_inferred
        ):
            pipe_outputs = self.outputs_dict[prerequisite_pipe]
            pipe_result = self._recursive_execute_pipe(prerequisite_pipe)
            if len(pipe_outputs) == 1:
                return_dict[pipe_outputs[0]] = pipe_result
            else:
                return_dict.update(pipe_result)

        return_dict = {k: return_dict[k] for k in self.final_outputs_inferred}
        return return_dict

    def visualize(self, full=False):

        nodes, edges = [], []  # dagre-py
        queue, visited = [], set()  # DFS

        for raw_input in self.raw_inputs:  # Init queue with raw input nodes
            nodes.append(
                {
                    "label": raw_input,
                    "id": raw_input,
                    "description": self._determine_description_dagre(
                        raw_input
                    ),
                }
            )
            visited.add(raw_input)
            for base_operator, inputs in self.inputs_dict.items():
                inputs = set(inputs)
                if raw_input in inputs:
                    queue.append((raw_input, base_operator))

        while queue:  # DFS until queue is empty
            src, target = queue.pop()
            edge = {
                "source": self._determine_object_id_for_dagre_node_label(src),
            }
            if full and isinstance(target, Pipe):
                edge[
                    "target"
                ] = self._determine_object_id_for_dagre_node_label(
                    target.steps[0]
                )
            else:
                edge[
                    "target"
                ] = self._determine_object_id_for_dagre_node_label(target)
            edges.append(edge)
            if target not in visited:
                output_dict_key = target
                if full and isinstance(target, Pipe):
                    target = self._add_pipe_steps_darge(target, edges, nodes)
                target_name = self._determine_object_name_for_dagre_node_label(
                    target
                )
                target_id = self._determine_object_id_for_dagre_node_label(
                    target
                )
                node = {
                    "label": target_name,
                    "id": target_id,
                    "description": self._determine_description_dagre(target),
                }
                nodes.append(node)
                visited.add(target)
                for output in self.outputs_dict[output_dict_key]:
                    output_node = {
                        "label": self._determine_object_name_for_dagre_node_label(
                            output
                        ),
                        "id": self._determine_object_id_for_dagre_node_label(
                            output
                        ),
                        "description": self._determine_description_dagre(
                            output
                        ),
                    }
                    nodes.append(output_node)
                    output_edge = {"source": target_id, "target": output}
                    edges.append(output_edge)
                    for base_operator, inputs in self.inputs_dict.items():
                        inputs = set(inputs)
                        if output in inputs:
                            queue.append((output, base_operator))

        plot({"nodes": nodes, "edges": edges})

    def _determine_object_name_for_dagre_node_label(self, obj):
        if isinstance(obj, Pipe):
            return obj.name
        elif isinstance(obj, BaseOperator):
            return type(obj).__name__
        elif obj in self.raw_inputs:
            return obj
        elif obj in set(self.all_outputs_list):
            return obj
        else:
            raise ValueError(f"Not sure how to name {obj} for Dagre-py")

    def _determine_object_id_for_dagre_node_label(self, obj):
        if isinstance(obj, BaseOperator):
            return str(obj)
        elif obj in self.raw_inputs:
            return obj
        elif obj in set(self.all_outputs_list):
            return obj
        else:
            raise ValueError(f"Not sure how to id {obj} for Dagre-py")

    def _add_pipe_steps_darge(self, pipe, edges, nodes):
        last_step = pipe.steps[-1]
        for i in range(1, len(pipe.steps)):
            prev, curr = pipe.steps[i - 1], pipe.steps[i]
            prev_name = self._determine_object_name_for_dagre_node_label(prev)
            prev_id = self._determine_object_id_for_dagre_node_label(prev)
            prev_node = {
                "label": prev_name,
                "id": prev_id,
                "description": self._determine_description_dagre(prev),
            }
            nodes.append(prev_node)
            edge = {
                "source": prev_id,
                "target": self._determine_object_id_for_dagre_node_label(curr),
            }
            edges.append(edge)
        return last_step

    def _determine_description_dagre(self, obj):
        if isinstance(obj, Pipe):
            description = (
                "Steps: "
                + " -> ".join([type(step).__name__ for step in obj.steps])
                + "\n\n"
            )
            for step in obj.steps:
                description += (
                    self._determine_object_name_for_dagre_node_label(step)
                    + "\n"
                )
                if step.__doc__:
                    description += step.__doc__ + "\n\n"
            return description
        elif isinstance(obj, BaseOperator):
            return obj.__doc__
        elif obj in self.raw_inputs:
            return obj
        elif obj in set(self.all_outputs_list):
            return obj
        else:
            raise ValueError(
                f"Not sure how to assign description for {obj} in Dagre-py"
            )
