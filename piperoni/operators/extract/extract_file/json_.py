from json import load

from pandas import DataFrame

from piperoni.operators.extract.extract_file.base import FileExtractor

"""
This module implements objects for extracting data from json files.
"""


class JSONExtractor(FileExtractor):
    """Extracts data from json files.

    json is interpreted as records, and there are options for flattening data
    and parsing tensor objects at leaf nodes of the json trees.

    Parameters
    ----------
    flatten : bool
        Flattens `list` and `dict` structures.
    express_tensors : bool
        Parses tensors at leaf-level values.
    delimiter : str
        Separates different json levels. Only used if flatten is set to `True`.
    kwargs : dict
        Keyword values used to customize extraction. Seejson.load for the
        accepted keywords.

    Caution
    -------
    An error will not be raised during instantiation if kwargs do not
    match the arguments in json.load, so be careful.
    """

    def __init__(self, flatten=True, express_tensors=True, sep="|", **kwargs):
        self.flatten = flatten
        self.express_tensors = express_tensors
        self.sep = sep
        self.kwargs = kwargs

    @property
    def output_type(self):
        return DataFrame

    def transform(self, path: str) -> DataFrame:
        """Returns a DataFrame representation of json data.

        Parameters
        ----------
        path : str
            Path to json file.

        Returns
        -------
        DataFrame
            Data contained in the file.
        """

        # loads json data as records
        with open(path, "r") as fp:
            data = load(fp, **self.kwargs)
            if not isinstance(data, list):
                data = [data]

        # returns records as a DataFrame
        if self.flatten:
            data = [self._flatten_json(record, self.sep) for record in data]
            if self.express_tensors:
                data = [
                    self._express_tensors(record, self.sep) for record in data
                ]
        return DataFrame.from_records(data)

    @staticmethod
    def _flatten_json(json, sep):
        """Flattens multilayered json data into one layer.

        Recursive implementation to obtain the flattened json.

        Parameters
        ----------
        json : json
            Input json data.
        sep : str
            Delimiter for nested data keys.

        Returns
        -------
        json
            Flattened representation of the data.
        """
        out = {}

        def flatten(element, name="", sep=sep):
            """Flatten one layer."""

            # for a dict type
            if isinstance(element, dict):
                for key, value in element.items():
                    flatten(element=value, name=name + key + sep)

            # for a list type
            elif isinstance(element, list):
                for key, value in enumerate(element):
                    flatten(element=value, name=name + str(key) + sep)

            # for singleton type
            else:
                name = name[:-1]  # removes the tail delimiter
                out[name] = element

        flatten(json)
        return out

    @staticmethod
    def _express_tensors(flat_json, sep):
        """Parses list-like values in leaf-nodes.

        Recursive implementation to obtain the condensed json.

        Parameters
        ----------
        flat_json : json
            Output of _flatten_json.
        sep : str
            Delimiter for nested data keys.

        Returns
        -------
        json
            Data where the end-lists have been condensed.
        """
        out = {}

        def condense(tree: dict, sep=sep):
            """Condense one layer."""

            keys = [i for i in tree.keys()]
            values = [i for i in tree.values()]
            tokens = [key.split(sep)[-1] for key in keys]
            keys_values_tokens = [i for i in zip(keys, values, tokens)]

            # updates persistant output with non-digit keys
            out.update(
                {i: j for i, j, k in keys_values_tokens if not k.isdigit()}
            )

            # checks if there are still lists to condense
            tree = {i: j for i, j, k in keys_values_tokens if k.isdigit()}
            if tree:

                # condenses individual lists
                condensed_tree = {}
                parent_keys = {
                    sep.join(i.split(sep)[:-1]) for i in tree.keys()
                }
                for substring in parent_keys:

                    # orders the list elements from each parent key
                    group = [(i, j) for i, j in tree.items() if substring in i]
                    group = sorted(
                        group, key=lambda x: int(x[0].split(sep)[-1])
                    )
                    condensed_tree[substring] = [j for i, j in group]

                # recursively apply condense on the newly condensed tree
                condense(tree=condensed_tree)

        condense(tree=flat_json)
        return out
