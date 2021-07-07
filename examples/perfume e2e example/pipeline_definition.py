from piperoni.operators.pipe import Pipe
from piperoni.operators.pipeline import Pipeline
import os
from operators import (CSVExtractor,
                    AddCategoricalClassInBlanks,
                    RegulatoryParsing,
                    AppendDataFrameCaster,
                    PrintOperator,
                    DropColumnsFromDataFrame,
                    ExportDFtoCSV,
                    AddDataFromListRandomly,
)

THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
print(THIS_FOLDER)

## ---------------------- ALL PIPES --------------------- ##

acetate_pipe = Pipe(
   [
      CSVExtractor(),
   ],
   name='Acetate Pipe'
)

aldehyde_pipe = Pipe(
    [
        CSVExtractor(),
    ],
    name = 'Aldehyde Pipe',
)

oil_pipe = Pipe(
    [
        CSVExtractor(),
    ],
    name = 'Oil Pipe',
)


phenol_pipe = Pipe(
    [
        CSVExtractor(),
    ],
    name = 'Phenol Pipe',
)


solvent_pipe = Pipe(
   [
      CSVExtractor(),
   ],
   name='Solvent Pipe'
)

formulations_pipe = Pipe(
    [
        CSVExtractor(),
        PrintOperator(),
        DropColumnsFromDataFrame(['oils', 'ketones', 'acetate', 'phenols', 'length']),
        AddDataFromListRandomly(column_name = "rpm", list_of_values = [20, 40, 60]),
        AddDataFromListRandomly(column_name = "mixing time", list_of_values = [1, 2, 3]),
        ExportDFtoCSV(output_filename = 'clean formulations file'),
    ]
)


ingredient_pipe = Pipe(
    [
    AppendDataFrameCaster(),
    RegulatoryParsing(),
    AddCategoricalClassInBlanks(['PROPERTY: Odor Strength', 'PROPERTY: Clean', 'PROPERTY: Floral', 'PROPERTY: Rose', 'PROPERTY: Medicinal',
                                'PROPERTY: Phenolic', 'PROPERTY: Spicy', 'PROPERTY: Sweet', 'PROPERTY: Vanilla', 'PROPERTY: Herbal', 'PROPERTY: Citrus', 'PROPERTY: Metallic',
                                'PROPERTY: Musk', 'PROPERTY: Sulfurous', 'PROPERTY: Bitter'], na_replacement_text = '0'),
    ExportDFtoCSV(output_filename = 'clean ingredients file'),
    ],
    name = 'Ingredient Pipe'
)


## ---------------------- PIPELINES --------------------- ##

inputs = {
   acetate_pipe: "raw acetate file",
   aldehyde_pipe: "raw aldehyde file",
   oil_pipe: "raw oil file",
   phenol_pipe: "raw phenol file",
   solvent_pipe: "raw solvent file",
   ingredient_pipe: ["clean acetate file",
                    "clean aldehyde file",
                    "clean oil file",
                    "clean phenol file",
                    "clean solvent file"],
    formulations_pipe: "raw formulations file",
}

outputs = {
   acetate_pipe: "clean acetate file",
   aldehyde_pipe: "clean aldehyde file",
   oil_pipe: "clean oil file",
   phenol_pipe: "clean phenol file",
   solvent_pipe: "clean solvent file",
   ingredient_pipe: "clean ingredient file",
   formulations_pipe: "clean formulations file",
}

raw_inputs = {
    "raw acetate file": THIS_FOLDER + '/ingest_files/acetate.csv',
    "raw aldehyde file": THIS_FOLDER + '/ingest_files/aldehyde.csv',
    "raw oil file": THIS_FOLDER + '/ingest_files/oil.csv',
    "raw phenol file": THIS_FOLDER + '/ingest_files/phenol.csv',
    "raw solvent file": THIS_FOLDER + '/ingest_files/solvent.csv',
    "raw formulations file": THIS_FOLDER + '/ingest_files/formulations.csv',
}


pipeline = Pipeline(inputs_dict = inputs, outputs_dict = outputs, raw_inputs = raw_inputs)
pipeline.visualize(full = True)
pipeline_output = pipeline.run()
# Interesting! pipeline_output can be called
ingredient_df = pipeline_output['clean ingredient file']