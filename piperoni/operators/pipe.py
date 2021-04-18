from functools import reduce

from typing import List

from abc import ABC, abstractmethod

from sys import exit
import warnings
import os

from piperoni.operators.base import BaseOperator
from piperoni.operators.analyze.comparer import CompareOperator
from piperoni.operators.load.checkpoint import Checkpoint
from piperoni.utils import datetime_to_prettystr

import logging
import pandas as pd
import numpy as np
import copy


class Pipe(BaseOperator):
    """A chain of lower-level operators.

    This is a linear pipe where the output of the first operator is passed
    as the input to the next operator.

    Parameters
    ----------
    steps: List[BaseOperator]
        The steps of the pipe, which are instances of BaseOperator.

    uid_column: str or None
        Column name for UIDs in any dataframes in the pipe for logging.

    logging_path: str or None
        The location to log the pipe.

    name: str
        Name of the pipe.

    stream_logging_level: int or logging.LEVEL, optional
        Level of logging desired for stream logging. Defaults to logging.INFO

    file_logging_level: int or logging.LEVEL, optional
        Level of logging desired for logfile. Defaults to logging.INFO

    autocompare: bool, optional
        Whether to turn on auto-comparing dataframes pre and post transforms.

    autocheckpoint: bool, optional
        Whether to turn on auto-checkpointing dataframes post transforms.

    Raises
    ------
    AssertionError
        If the steps passed to the pipe are not instances of BaseOperator.
    """

    def __init__(
        self,
        steps: List[BaseOperator],
        uid_column=None,
        logging_path=None,
        name=None,
        stream_logging_level=logging.INFO,
        file_logging_level=None,
        autocompare=False,
        autocheckpoint=False,
    ) -> None:

        # Instance variables
        self.steps = steps
        self.uid_column = uid_column
        self.logging_path = logging_path
        if name:
            self.name = name
        else:
            self.name = "Pipe"
        self.stream_logging_level = stream_logging_level
        self.file_logging_level = file_logging_level
        self.autocompare = autocompare
        self.autocheckpoint = autocheckpoint

        # Set up logging
        self._setup_logging()

        # Verify pipe
        self._verify_pipe()

    def transform(self, input_: object) -> object:
        """Apply the pipe steps to the input data.

        Parameters
        ----------
        input_: object
            The input data passed to the first step in the pipe.

        Returns
        -------
        object
            The output of the final step of the pipe.
        """
        f = logging.Formatter(
            f"{self._tag} - {self.__class__.__name__}: %(message)s"
        )
        for handler in self.logger.handlers:
            handler.setFormatter(f)
        self.logger.info(f"Starting execution of pipe.")
        for i, step in enumerate(self.steps):
            loggable_transform = self._autologable(input_)
            if loggable_transform:
                autocompare_transform = CompareOperator(
                    input_, self.uid_column
                )
            input_ = self._apply(step, input_)
            if loggable_transform:
                input_ = self._apply(autocompare_transform, input_)

            # TODO: Allow custom checkpointers here.
            if isinstance(input_, pd.DataFrame) and self.autocheckpoint:
                autosave_transform = Checkpoint(
                    os.path.join(
                        self.logging_path, f"{self.timestamp}_{i}.csv"
                    )
                )
                input_ = self._apply(autosave_transform, input_)

        f = logging.Formatter(
            f"{self._tag} - {self.__class__.__name__}: %(message)s"
        )
        for handler in self.logger.handlers:
            handler.setFormatter(f)
        self.logger.info(f"Ended execution of pipe.")
        return input_

    @property
    def _tag(self):
        """Create a timestamped unique name for pipe"""
        self.timestamp = datetime_to_prettystr()
        if self.name is not None:
            tag = self.timestamp + " - " + self.name
        else:
            tag = self.timestamp + " - " + self.__class__.__name__
        return tag

    def _setup_logging(self):
        """Sets up logging based on pipe options. Initializes a stream logger and a file logger."""

        # Checkpointing needs path
        if (self.autocheckpoint) and (self.logging_path is None):
            raise RuntimeError(
                "Autocheckpointing requires a file path. Please provide a logging_path variable."
            )

        # File logging needs path
        if (self.file_logging_level is not None) and (
            self.logging_path is None
        ):
            raise RuntimeError(
                "Logging to file requires a file path. Please provide a logging_path variable."
            )

        # Set up global logging stuff
        # root_logger = logging.getLogger()
        # root_logger.handlers = []

        # Set up logger
        self.logger.setLevel(10)
        self.logger.handlers = []

        # Set up file handler
        if self.file_logging_level:
            self.filehandler = logging.FileHandler(
                os.path.join(self.logging_path, self._tag + ".txt"), "w"
            )
            self.filehandler.setLevel(self.file_logging_level)
            self.logger.handlers.append(self.filehandler)

        # Set up stream handler
        self.streamhandler = logging.StreamHandler()
        self.streamhandler.setLevel(self.stream_logging_level)
        self.logger.handlers.append(self.streamhandler)

        f = logging.Formatter(
            f"{self._tag} - {self.__class__.__name__}: %(message)s"
        )
        for handler in self.logger.handlers:
            handler.setFormatter(f)

    def _verify_pipe(self):
        self.logger.info(f"Pipe initialized.")
        self.logger.info(f"The pipe consists of the following transforms:")
        for step in self.steps:
            if not isinstance(step, BaseOperator):
                raise RuntimeError(
                    "Pipe steps must be a subclass of BaseOperator"
                )
            self.logger.info(f"{step.__class__.__name__}")

    def _apply(self, transform, input_):
        """
        Apply a single transform with logging.

        Parameters
        ----------
        transform : BaseOperator
            Transform to apply.
        input_ : Any
            Object to apply transform to.

        Returns
        -------
        Any
            Post-transform object.
        """
        transform_name = transform.__class__.__name__
        f = logging.Formatter(f"  {self._tag} - {transform_name}: %(message)s")
        for handler in self.logger.handlers:
            handler.setFormatter(f)
        self.logger.info(f"Applying transform")
        try:
            output = transform(input_)
        except Exception as caught_exception:
            self.logger.error(f"Fatal error encountered in transform:")
            self.logger.exception(f"{caught_exception}")
            raise caught_exception
        self.logger.info(f"Applied transform")

        return output

    def _autologable(self, input_):
        if (
            (isinstance(input_, pd.DataFrame))
            and (self.uid_column)
            and (self.autocompare)
            and (self.uid_column in input_.columns)
        ):
            return True
        return False
