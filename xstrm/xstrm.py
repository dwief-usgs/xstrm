"""Use xstrm modules to create stream summaries."""

from xstrm import build_network
from xstrm import network_calc
import os.path
import sys


def network_calc_to_csv(
        to_from_csv,
        local_data_csv,
        id_col_name,
        to_node_col,
        from_node_col,
        weight_col_name=None,
        calc_type="sum",
        include_seg=True,
        include_missing=True,
        num_proc=4,
        precision=3
):
    """Use xstrm package to process stream network summaries.

    Description
    ----------
    Use xstrm methods to process network summaries.
    This function builds a network from to from nodes
    and calculates network summaries for each segment,
    where a segment can be a stream segment of catchment
    associated with the nodes. Exports data to csv.

    Parameters
    ----------
    to_from_csv: str
        string representation of file name including directory
        and extension. this file should contain three fields
        including 1)segment identifier, 2)to Node, 3)from Node
        e.g. 'data/my_network_data.csv'
    local_data_csv: list
        list of column names to perform summary calculations on
    id_col_name: str
        string representation of name of the identifier column
        values in this column can represent str or num
    to_node_col: str
        string representation of name of the column containing
        to node information for the network
        values in this column can represent str or num
    from_node_col: str
        string representation of name of the column containing
        from node information for the network
        values in this column can represent str or num
    weight_col_name: str
        name of the column containing weights for upstream average
        this field is optional, default all segments have equal
        weights. Values should be int or float
    calc_type: str
        options: 'sum','min','max','weighted_avg'
        see network_calc.calc_* functions for more detail
    include_seg: bool
        where True means add segment to parent list
    include_missing: bool
        True include percent of segs (using weight) without data
        at the local scale
    num_proc: int
        Number of worker processes to use in multiprocessing
    precision: int
        number of decimal places to round float data

    Returns
    ----------
    file: csv
        dictionary of calculated missing values for each variable
        does not summarize ids or seg_weight

    Note
    ----------
    for NHD networks the to node is the downstream node

    """
    if os.path.isfile(to_from_csv) and os.path.isfile(local_data_csv):
        pass
    else:
        sys.exit(
            "Please verify file path and name. Include extension .csv"
        )

    in_dir, in_file = os.path.split(local_data_csv)

    local_df = network_calc.get_local_vars(
        local_data_csv, id_col_name, weight_col_name
    )

    build_network_data = build_network.import_tofrom(
        to_from_csv, id_col_name, to_node_col, from_node_col
    )

    traverse_queue = build_network.build_network_setup(
        build_network_data
    )

    summary = network_calc.build_network(
        traverse_queue, include_seg, num_proc=num_proc
    )

    summary.add_processing_details(
        local_df, calc_type, include_missing
    )

    summary.calc_one_parent()
    summary.calc_no_parent()
    summary.calc_mult_parent_mp()
    summary.combine_dfs()
    # rename index to be initial id submitted
    final_df = summary.final_df.round(precision)
    final_df.index.rename(id_col_name, inplace=True)

    if in_dir == "":
        out_name = f"{in_dir}n_{calc_type}_{in_file}"
    else:
        out_name = f"{in_dir}/n_{calc_type}_{in_file}"
    final_df.to_csv(out_name)

