import pandas as pd
import os
from piperoni.operators.extract.extract_file.csv_ import CSVExtractor
from piperoni.operators.transform_operator import TransformOperator
from piperoni.operators.cast_operator import CastOperator
from piperoni.operators.passthrough_operator import PassthroughOperator
from piperoni.operators.pipeline import PipelineData
from random import sample



THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
print(THIS_FOLDER)

## Let Piperoni begin. Start with Acetate


class AddCategoricalClassInBlanks(TransformOperator):
    """
    Replaces blank spaces in a component csv with 0s.
    """

    def __init__(self, column_headers, na_replacement_text):
        self.column_headers = column_headers
        self.na_replacement_text = na_replacement_text

    def transform(self, input_) -> pd.DataFrame:
        df = input_
        for column_names in self.column_headers:
            df[column_names].fillna(self.na_replacement_text, inplace = True)
        return df

class RegulatoryParsing(TransformOperator):
    """
    Extracts the type of regulatory appendix.
    """
    def regulatory_parsing(self, restriction)-> str:
        try:
            regulation = restriction.split('/')[0]
            return (regulation)
        except:
            return ('No Regulations')

    def transform(self, input_) -> pd.DataFrame:
        df = input_
        df['PROPERTY: Regulatory Code'] = df['PROPERTY: Regulations'].apply(self.regulatory_parsing)
        return df

class PrintOperator(PassthroughOperator):
   def transform(self, input_):
      print(input_)
      return input_

class AppendDataFrameCaster(CastOperator):

# This is a validation step
    @property
    def input_type(self):
        return PipelineData

    @property
    def output_type(self):
        return pd.DataFrame


    def transform(self, input_) -> pd.DataFrame:
        output_df = pd.concat(input_.values(), axis = 0)
        return output_df

class ExportDFtoCSV(PassthroughOperator):

    """
    Write the data in CSV format to data/ingest_files/file-name.csv
    """

    def __init__(self, output_filename):
        self.output_filename = output_filename

    def transform(self, input_):
        print(input_)
        input_.to_csv(THIS_FOLDER + '/ingest_files/' + self.output_filename + '.csv')
        return input_


class DropColumnsFromDataFrame(TransformOperator):
    """
    Drops columns that are no longer necessary
    """

    def __init__(self, column_headers):
        self.column_headers = column_headers

    def transform(self, input_) -> pd.DataFrame:
        df = input_
        for column_names in self.column_headers:
            df.drop(column_names, inplace = True, axis = 1)
        return df

class AddDataFromListRandomly(TransformOperator):
    """
    Add data in a column by randomly sampling from a list
    """

    def __init__(self, column_name, list_of_values):
        self.column_name = column_name
        self.list_of_values = list_of_values

    def transform(self, input_) -> pd.DataFrame:
        df = input_
        print(self.column_name)
        print(self.list_of_values)
        df[self.column_name] = df.apply(lambda row: sample(self.list_of_values, 1)[0], axis = 1)
        return df

