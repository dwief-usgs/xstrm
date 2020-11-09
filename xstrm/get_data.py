"""Intended to help accept and format data for upstream calculations.

    Author
    ----------
    Daniel Wieferich: dwieferich@usgs.gov

    Description
    ----------
    User supplies a csv or txt file the following format.

    Notes
    ----------
    These methods are limited to...
"""

# Import needed packages
import pandas as pd


def import_network_data(file_name,
                        id_col_name='seg_id',
                        up_col_name='up_seg_id'):
    """Import and format text file with stream network info.

    Description
    ----------
    Imports text file into pandas that documents relationships
    of the stream network through pairs of identifiers.
    (i.e.segment id, upstream segment id)
    We suggest to use field names seg_id and up_seg_id in string format
    but the function will attempt to use existing names and formats.

    Output intended for use in network_summary.py of the summarize_upstream
    module

    Parameters
    ----------
    file_name: str
        string representation of file name including directory and extension
        e.g. 'data/my_network_data.csv'
    id_col_name: str
        name of the identifier column
        values in this column can represent str or int
    up_col_name: str
        name of the upstream identifier column
        values in this column can represent str or int

    Returns
    ----------
    df2: df
        pandas dataframe intended for use in
        network_summary.py of the summarize_upstream

    """
    field_names = {str(id_col_name): "seg_id",
                   str(up_col_name): "up_seg_id"}

    df = pd.read_csv(file_name,
                     usecols=[str(id_col_name), str(up_col_name)],
                     encoding="iso-8859-1")

    df2 = df.rename(field_names, axis="columns")
    df2 = df2.fillna(0)

    return df2


def import_local_data(file_name,
                      id_col_name='seg_id',
                      weight_col_name=None):
    """Import and format text file with local segment data.

    Description
    ----------
    Imports text file into pandas that contains summaries of information
    at the local stream segment level.  This file should also contain the
    stream segment identifier and stream_segment weight (e.g. area for
    area weighted average or length for length weighted average.) If weight
    is not required use default.

    See file 'tests/test_local_data.csv' as example

    Parameters
    ----------
    file_name: str
        string representation of file name including directory and extension
        e.g. 'data/my_local_data.csv'
    id_col_name: str
        name of the identifier column
        values in this column can represent str or int
    weight_col_name: str
        name of the column containing weights for upstream average
        this field is not required
        if used values should be int or float

    Returns
    ----------
    df2: df
        pandas dataframe intended for use in
        network_summary.py of the summarize_upstream module

    """
    field_names = {str(id_col_name): "seg_id"}
    df = pd.read_csv(file_name, encoding="iso-8859-1")

    if weight_col_name is not None:
        field_names[str(weight_col_name)] = "seg_weight"
    else:
        df['seg_weight'] = 1

    df2 = df.rename(field_names, axis="columns")

    return df2
