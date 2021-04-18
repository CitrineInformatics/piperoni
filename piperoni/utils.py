import pandas as pd
import copy
import warnings
import datetime as dt


def flatten_dict(d):
    """function to flatten a dictionary into a single layer

    Parameters
    ----------
    d : dict
        multilayered dictionary

    Returns
    -------
    dict
        flattened dictionary
    """
    out = {}

    def flatten(x, name="", sep="|"):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + sep)
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + sep)
                i += 1
        else:
            out[str(name[:-1])] = x

    flatten(d)
    return out


def compare_columns(df1: pd.DataFrame, df2: pd.DataFrame):
    """
    Helper function.
    Compares the columns of a dataframe to another,
    returning the overlapping columns, cols only in df1, and cols only in df2.

    Parameters
    ----------
    df1: pd.DataFrame
    df2: pd.DataFrame

    Returns
    -------
    overlaps, df1 unique columns, df2 unique columns
    """
    cols1 = set(df1.columns)
    cols2 = set(df2.columns)
    overlaps = cols1 & cols2
    return overlaps, cols1 - overlaps, cols2 - overlaps


def compare_dataframes(
    df_input: pd.DataFrame,
    df_reference: pd.DataFrame,
    unique_id_col: str,
    ignore_cols=None,
):
    """
    Compares an incoming dataset to a reference dataset.
    First checks if columns are consistent.
    Then, checks whether if all entries in the reference are present in the incoming dataset
    by comparing the unique entries.
    Then, checks that the values are consistent from the input to the reference for all
    columns that are present in both.
    Finally, reports newly added rows.

    Parameters
    ----------
    input: pd.DataFrame
        Dataset to compare to reference
    reference: pd.DataFrame
        Dataset to be used as the reference
    unique_id_col: str
        Name of the column that can be used as a unique identifier. This
        allows comparison of rows for any changes between the input and
        the reference.
    ignore_cols: str or None, optional
        Name of column(s) to ignore in analysis
    """

    # First, copy stuff so no permanent changes
    df_input = copy.deepcopy(df_input)
    df_reference = copy.deepcopy(df_reference)

    # Obtain overlapping columns, columns unique to input, and colums unique
    # to the reference.
    (
        overlapping_cols,
        unique_input,
        unique_reference,
    ) = compare_columns(df_input, df_reference)
    if unique_input:
        warnings.warn(
            f"{unique_input} found in the input dataset, but not in the reference."
        )
    if unique_reference:
        warnings.warn(
            f"{unique_reference} found in the reference dataset, but not in the input."
        )
    # If unique in column in missing from either dataset, stop execution.
    if unique_id_col not in overlapping_cols:
        warnings.warn(
            f"Provided unique ID column {unique_id_col} does not exist in both datasets!"
        )

    # Filter out columns to ignore and the unique id columns, then make loopable
    overlapping_cols = overlapping_cols - {unique_id_col} - {ignore_cols}
    overlapping_cols = list(overlapping_cols)

    # Obtain list of unique ids, then turn the unique id column into index
    # for convenience and faster access
    ref_unique_entries = df_reference[unique_id_col].tolist()
    input_unique_entries = df_input[unique_id_col].tolist()
    reindexed_input = df_input.set_index(unique_id_col)
    reindexed_ref = df_reference.set_index(unique_id_col)

    # Setup flag that detects and inconsistencies. Changes to true if
    # differences are found
    mismatch_found = False
    for entry in ref_unique_entries:
        try:
            if (
                not reindexed_input[overlapping_cols]
                .loc[entry]
                .equals(reindexed_ref[overlapping_cols].loc[entry])
            ):
                mismatch_found = True
                mismatches = (
                    ~reindexed_input[overlapping_cols]
                    .loc[entry]
                    .fillna("dummy")
                    .eq(
                        reindexed_ref[overlapping_cols]
                        .loc[entry]
                        .fillna("dummy")
                    )
                    .values
                )
                warnings.warn(
                    f"Row {entry} has values {reindexed_input[overlapping_cols].loc[entry][mismatches].values} in input data, but has values {reindexed_ref[overlapping_cols].loc[entry][mismatches].values} in the reference for columns {reindexed_ref[overlapping_cols].columns[mismatches].values}"
                )
        except KeyError:
            mismatch_found = True
            warnings.warn(
                f"Row {entry} in reference dataset not found in the input!"
            )

    if not mismatch_found:
        warnings.warn(
            "All rows in input are consistent with the reference in overlapping columns."
        )

    if set(input_unique_entries) - set(ref_unique_entries):
        warnings.warn(
            f"The following rows are present in the input but not in the reference:\n{set(input_unique_entries) - set(ref_unique_entries)}"
        )
    else:
        warnings.warn("No new rows found in the input.")


def datetime_to_prettystr(style="datetime"):
    datetime = str(dt.datetime.now())
    datetime = datetime.replace(" ", "_")
    datetime = datetime.split(".")[0]
    datetime = datetime.replace(":", ".")
    if style == "date":
        return datetime[0:10]
    if style == "time":
        return datetime[11:]
    return datetime
